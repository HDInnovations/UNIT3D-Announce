use axum::{
    async_trait,
    extract::{ConnectInfo, FromRef, FromRequestParts, Path, State},
    http::{
        header::{ACCEPT_CHARSET, ACCEPT_LANGUAGE, REFERER, USER_AGENT},
        request::Parts,
        HeaderMap,
    },
};
use chrono::{DateTime, Duration};
use rand::{rngs::SmallRng, seq::IteratorRandom, Rng, SeedableRng};
use sqlx::types::chrono::Utc;
use std::{
    fmt::Display,
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::Arc,
};
use tokio::net::TcpStream;

use crate::error::AnnounceError::{
    self, AbnormalAccess, BlacklistedClient, BlacklistedPort, DownloadPrivilegesRevoked,
    GroupBanned, GroupDisabled, GroupNotFound, GroupValidating, InfoHashNotFound,
    InternalTrackerError, InvalidCompact, InvalidDownloaded, InvalidInfoHash, InvalidLeft,
    InvalidNumwant, InvalidPasskey, InvalidPeerId, InvalidPort, InvalidQueryStringKey,
    InvalidQueryStringValue, InvalidUploaded, InvalidUserAgent, MissingDownloaded, MissingInfoHash,
    MissingLeft, MissingPeerId, MissingPort, MissingUploaded, NotAClient, PasskeyNotFound,
    PeersPerTorrentPerUserLimit, StoppedPeerDoesntExist, TorrentIsDeleted,
    TorrentIsPendingModeration, TorrentIsPostponed, TorrentIsRejected, TorrentNotFound,
    TorrentUnknownModerationStatus, UnsupportedEvent, UserAgentTooLong, UserNotFound,
};

use crate::tracker::{
    self,
    connectable_port::ConnectablePort,
    featured_torrent::FeaturedTorrent,
    freeleech_token::FreeleechToken,
    peer::{self, Peer, PeerId},
    personal_freeleech::PersonalFreeleech,
    torrent::InfoHash,
    user::Passkey,
    Tracker,
};
use crate::utils;

#[derive(Clone, Copy, PartialEq, Default)]
pub enum Event {
    Completed,
    #[default]
    Empty,
    Started,
    Stopped,
}

impl Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Completed => write!(f, "completed"),
            Self::Empty => write!(f, ""),
            Self::Started => write!(f, "started"),
            Self::Stopped => write!(f, "stopped"),
        }
    }
}

impl FromStr for Event {
    type Err = AnnounceError;

    fn from_str(event: &str) -> Result<Self, AnnounceError> {
        match event {
            "" | "empty" | "paused" => Ok(Self::Empty),
            "completed" => Ok(Self::Completed),
            "started" => Ok(Self::Started),
            "stopped" => Ok(Self::Stopped),
            _ => Err(UnsupportedEvent),
        }
    }
}

pub struct Announce {
    info_hash: InfoHash,
    peer_id: PeerId,
    port: u16,
    uploaded: u64,
    downloaded: u64,
    left: u64,
    event: Event,
    numwant: usize,
    corrupt: Option<u64>,
    key: Option<String>,
}

pub struct Query<T>(pub T);

/// Extracts the query parameters in the HTTP GET request.
#[async_trait]
impl<S> FromRequestParts<S> for Query<Announce>
where
    S: Send + Sync,
    Arc<Tracker>: FromRef<S>,
{
    type Rejection = AnnounceError;

    async fn from_request_parts(parts: &mut Parts, tracker: &S) -> Result<Self, Self::Rejection> {
        let query_string = parts.uri.query().unwrap_or_default();
        let query_bytes = query_string.as_bytes();
        let query_length = query_bytes.len();
        let mut pos = 0;
        let mut ampersand_positions = memchr::memchr_iter(b'&', query_bytes);

        let mut info_hash: Option<InfoHash> = None;
        let mut peer_id: Option<PeerId> = None;
        let mut port: Option<u16> = None;
        let mut uploaded: Option<u64> = None;
        let mut downloaded: Option<u64> = None;
        let mut left: Option<u64> = None;
        let mut event: Option<Event> = None;
        let mut numwant: Option<usize> = None;
        let mut corrupt: Option<u64> = None;
        let mut key: Option<String> = None;

        for equal_sign_pos in memchr::memchr_iter(b'=', query_bytes) {
            let value_end_pos = ampersand_positions.next().unwrap_or(query_length);

            let parameter = query_string
                .get(pos..equal_sign_pos)
                .ok_or(InvalidQueryStringKey)?;
            let value = query_string
                .get(equal_sign_pos + 1..value_end_pos)
                .ok_or(InvalidQueryStringValue)?;

            match parameter {
                "info_hash" => {
                    info_hash = Some(InfoHash::from(
                        utils::urlencoded_to_bytes(value)
                            .await
                            .or(Err(InvalidInfoHash))?,
                    ))
                }
                "peer_id" => {
                    peer_id = Some(PeerId::from(
                        utils::urlencoded_to_bytes(value)
                            .await
                            .or(Err(InvalidPeerId))?,
                    ))
                }
                "port" => port = Some(value.parse().or(Err(InvalidPort))?),
                "uploaded" => uploaded = Some(value.parse().or(Err(InvalidUploaded))?),
                "downloaded" => downloaded = Some(value.parse().or(Err(InvalidDownloaded))?),
                "left" => left = Some(value.parse().or(Err(InvalidLeft))?),
                "compact" => {
                    if value != "1" {
                        return Err(InvalidCompact);
                    }
                }
                "event" => event = Some(value.parse()?),
                "numwant" => numwant = Some(value.parse().or(Err(InvalidNumwant))?),
                "corrupt" => corrupt = value.parse().ok(),
                "key" => key = value.parse().ok(),
                _ => (),
            }

            if value_end_pos == query_length {
                break;
            } else {
                pos = value_end_pos + 1;
            }
        }

        let State(tracker): State<Arc<Tracker>> = State::from_request_parts(parts, tracker)
            .await
            .or(Err(InternalTrackerError))?;

        Ok(Query(Announce {
            info_hash: info_hash.ok_or(MissingInfoHash)?,
            peer_id: peer_id.ok_or(MissingPeerId)?,
            port: port.ok_or(MissingPort)?,
            uploaded: uploaded.ok_or(MissingUploaded)?,
            downloaded: downloaded.ok_or(MissingDownloaded)?,
            left: left.ok_or(MissingLeft)?,
            event: event.unwrap_or_default(),
            numwant: {
                if event.unwrap_or_default() == Event::Stopped {
                    0
                } else {
                    numwant
                        .unwrap_or(tracker.config.numwant_default)
                        .min(tracker.config.numwant_max)
                }
            },
            corrupt,
            key,
        }))
    }
}

pub struct ClientIp(pub std::net::IpAddr);

#[async_trait]
impl FromRequestParts<Arc<Tracker>> for ClientIp {
    type Rejection = AnnounceError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<Tracker>,
    ) -> Result<Self, Self::Rejection> {
        let ip = if let Some(header) = &state.config.reverse_proxy_client_ip_header_name {
            // We need the right-most ip, which should be included in the last header
            parts
                .headers
                .get_all(header)
                .iter()
                .last()
                // Err: Missing the client ip header
                .ok_or(InternalTrackerError)?
                .to_str()
                // Err: Client ip header is not UTF-8
                .map_err(|_| InternalTrackerError)?
                .split(',')
                .last()
                // Err: Client ip header is empty
                .ok_or(InternalTrackerError)?
                .trim()
                .parse()
                // Err: Client ip header does not contain a valid ip address
                .map_err(|_| InternalTrackerError)?
        } else {
            // If the header isn't configured, use the connecting ip.
            let ConnectInfo(addr) = ConnectInfo::<SocketAddr>::from_request_parts(parts, state)
                .await
                .or(Err(InternalTrackerError))?;

            addr.ip()
        };

        Ok(ClientIp(ip))
    }
}

pub async fn announce(
    State(tracker): State<Arc<Tracker>>,
    Path(passkey): Path<String>,
    Query(queries): Query<Announce>,
    headers: HeaderMap,
    ClientIp(client_ip): ClientIp,
) -> Result<Vec<u8>, AnnounceError> {
    // Validate headers
    if headers.contains_key(ACCEPT_LANGUAGE)
        || headers.contains_key(REFERER)
        || headers.contains_key(ACCEPT_CHARSET)
        // This header check may block Non-bittorrent client `Aria2` to access tracker,
        // Because they always add this header which other clients don't have.
        //
        // See: https://blog.rhilip.info/archives/1010/ ( in Chinese )
        || headers.contains_key("want-digest")
    {
        return Err(AbnormalAccess);
    }

    // User agent header is required.
    let user_agent = headers
        .get(USER_AGENT)
        .ok_or(InvalidUserAgent)?
        .to_str()
        .or(Err(InvalidUserAgent))?;

    // Block user agent strings that are too long. (For Database reasons)
    if user_agent.len() > 64 {
        return Err(UserAgentTooLong);
    }

    // Block user agent strings on the blacklist
    for client in tracker.agent_blacklist.read().iter() {
        if queries.peer_id.starts_with(&client.peer_id_prefix) {
            return Err(BlacklistedClient);
        }
    }

    // Block user agent strings on the regex blacklist
    let user_agent_lower = user_agent.to_ascii_lowercase();

    if user_agent_lower.contains("mozilla")
        || user_agent_lower.contains("browser")
        || user_agent_lower.contains("chrome")
        || user_agent_lower.contains("safari")
        || user_agent_lower.contains("applewebkit")
        || user_agent_lower.contains("opera")
        || user_agent_lower.contains("links")
        || user_agent_lower.contains("lynx")
        || user_agent_lower.contains("bot")
        || user_agent_lower.contains("unknown")
    {
        return Err(NotAClient);
    }

    // Validate port
    // Some clients send port 0 on the stopped event
    if tracker.port_blacklist.read().contains(&queries.port) && queries.event != Event::Stopped {
        return Err(BlacklistedPort);
    }

    let passkey: Passkey = Passkey::from_str(&passkey).or(Err(InvalidPasskey))?;

    // Validate passkey
    let user_id = tracker
        .passkey2id
        .read()
        .get(&passkey)
        .ok_or(PasskeyNotFound)
        .cloned();
    let user = tracker
        .users
        .read()
        .get(&user_id.unwrap_or(0))
        .ok_or(UserNotFound)
        .cloned();

    // Validate torrent
    let torrent_id = *tracker
        .infohash2id
        .read()
        .get(&queries.info_hash)
        .ok_or(InfoHashNotFound)?;

    let (
        upload_factor,
        download_factor,
        uploaded_delta,
        downloaded_delta,
        seeder_delta,
        leecher_delta,
        times_completed_delta,
        is_visible,
        user,
        user_id,
        group,
        response,
    ) = {
        let mut torrent_guard = tracker.torrents.lock();
        let torrent = torrent_guard.get_mut(&torrent_id).ok_or(TorrentNotFound)?;

        if torrent.is_deleted {
            return Err(TorrentIsDeleted);
        }

        match torrent.status {
            tracker::torrent::Status::Approved => (),
            tracker::torrent::Status::Pending => return Err(TorrentIsPendingModeration),
            tracker::torrent::Status::Rejected => return Err(TorrentIsRejected),
            tracker::torrent::Status::Postponed => return Err(TorrentIsPostponed),
            _ => return Err(TorrentUnknownModerationStatus),
        }

        let user_id = user_id?;
        let user = user?;

        // Validate user
        if !user.can_download && queries.left != 0 {
            return Err(DownloadPrivilegesRevoked);
        }

        let group = tracker
            .groups
            .read()
            .get(&user.group_id)
            .ok_or(GroupNotFound)?
            .clone();

        match group.slug.as_str() {
            "banned" => return Err(GroupBanned),
            "validating" => return Err(GroupValidating),
            "disabled" => return Err(GroupDisabled),
            _ => (),
        }

        // Make sure user isn't leeching more torrents than their group allows unless they are a lifetime user
        let has_hit_download_slot_limit = queries.left > 0
            && !user.is_lifetime
            && matches!(group.download_slots, Some(slots) if slots <= user.num_leeching);

        // Change of upload/download compared to previous announce
        let uploaded_delta;
        let downloaded_delta;
        let seeder_delta;
        let leecher_delta;
        let times_completed_delta;
        let mut updated_at: Option<DateTime<Utc>> = None;
        let is_visible;

        if queries.event == Event::Stopped {
            // Try and remove the peer
            let removed_peer = torrent.peers.swap_remove(&tracker::peer::Index {
                user_id,
                peer_id: queries.peer_id,
            });
            // Check if peer was removed
            if let Some(peer) = removed_peer {
                // Calculate change in upload and download compared to previous
                // announce
                uploaded_delta = queries.uploaded.saturating_sub(peer.uploaded);
                downloaded_delta = queries.downloaded.saturating_sub(peer.downloaded);

                leecher_delta = 0 - peer.is_included_in_leech_list() as i32;
                seeder_delta = 0 - peer.is_included_in_seed_list() as i32;
            } else {
                return Err(StoppedPeerDoesntExist);
            }

            times_completed_delta = 0;
            is_visible = false;
        } else {
            // Insert the peer into the in-memory db
            let mut old_peer: Option<Peer> = None;
            let new_peer = *torrent
                .peers
                .entry(tracker::peer::Index {
                    user_id,
                    peer_id: queries.peer_id,
                })
                .and_modify(|peer| {
                    old_peer = Some(*peer);

                    peer.ip_address = client_ip;
                    peer.port = queries.port;
                    peer.is_seeder = queries.left == 0;
                    peer.is_visible =
                        peer.is_included_in_leech_list() || !has_hit_download_slot_limit;
                    peer.is_active = true;
                    peer.updated_at = Utc::now();
                    peer.uploaded = queries.uploaded;
                    peer.downloaded = queries.downloaded;
                })
                .or_insert(tracker::Peer {
                    ip_address: client_ip,
                    user_id,
                    torrent_id,
                    port: queries.port,
                    is_seeder: queries.left == 0,
                    is_active: true,
                    is_visible: !has_hit_download_slot_limit,
                    updated_at: Utc::now(),
                    uploaded: queries.uploaded,
                    downloaded: queries.downloaded,
                });

            is_visible = new_peer.is_visible;

            // Update the user and torrent seeding/leeching counts in the
            // in-memory db
            match old_peer {
                Some(old_peer) => {
                    leecher_delta = new_peer.is_included_in_leech_list() as i32
                        - old_peer.is_included_in_leech_list() as i32;
                    seeder_delta = new_peer.is_included_in_seed_list() as i32
                        - old_peer.is_included_in_seed_list() as i32;
                    times_completed_delta = (new_peer.is_seeder && !old_peer.is_seeder) as u32;

                    // Calculate change in upload and download compared to previous
                    // announce
                    if queries.uploaded < old_peer.uploaded
                        || queries.downloaded < old_peer.downloaded
                    {
                        // Client sent the same peer id but restarted the session
                        // Assume delta is 0
                        uploaded_delta = 0;
                        downloaded_delta = 0;
                    } else {
                        // Assume client continues previously tracked session
                        uploaded_delta = queries.uploaded - old_peer.uploaded;
                        downloaded_delta = queries.downloaded - old_peer.downloaded;
                    }

                    updated_at = Some(old_peer.updated_at);
                }
                None => {
                    // new peer is inserted

                    // Make sure user is only allowed N peers per torrent.
                    let mut peer_count = 0;

                    for &peer in torrent.peers.values() {
                        if peer.user_id == user_id && peer.is_included_in_peer_list() {
                            peer_count += 1;

                            if peer_count >= tracker.config.max_peers_per_torrent_per_user {
                                torrent.peers.swap_remove(&tracker::peer::Index {
                                    user_id,
                                    peer_id: queries.peer_id,
                                });

                                return Err(PeersPerTorrentPerUserLimit(
                                    tracker.config.max_peers_per_torrent_per_user,
                                ));
                            }
                        }
                    }

                    leecher_delta = new_peer.is_included_in_leech_list() as i32;
                    seeder_delta = new_peer.is_included_in_seed_list() as i32;
                    times_completed_delta = 0;

                    // Calculate change in upload and download compared to previous
                    // announce
                    uploaded_delta = 0;
                    downloaded_delta = 0;
                }
            }
        }

        // Has to be adjusted before the peer list is generated
        torrent.seeders = torrent.seeders.saturating_add_signed(seeder_delta);
        torrent.leechers = torrent.leechers.saturating_add_signed(leecher_delta);
        torrent.times_completed = torrent
            .times_completed
            .saturating_add(times_completed_delta);

        let warning_opt = if updated_at.is_some_and(|updated_at| {
            updated_at
                .checked_add_signed(Duration::seconds(tracker.config.announce_min.into()))
                .is_some_and(|blocked_until| blocked_until > Utc::now())
        }) {
            // Peer last announced less than announce_min seconds ago
            Some("Rate limit exceeded. Please wait.".to_string())
        } else if !is_visible {
            // User has full download slots
            Some("Download slot limit reached.".to_string())
        } else {
            None
        };

        // Generate peer lists to return to client

        let mut peers_ipv4: Vec<u8> = Vec::new();
        let mut peers_ipv6: Vec<u8> = Vec::new();

        // Only provide peer list if
        // - it is not a stopped event,
        // - there exist leechers (we have to remember to update the torrent leecher count before this check)
        // - there is no warning in the response
        if queries.event != Event::Stopped && torrent.leechers > 0 && warning_opt.is_none() {
            let mut peers: Vec<(&peer::Index, &Peer)> = Vec::with_capacity(std::cmp::min(
                queries.numwant,
                torrent.seeders as usize + torrent.leechers as usize,
            ));

            // Don't return peers with the same user id or those that are marked as inactive
            let valid_peers = torrent.peers.iter().filter(|(_index, peer)| {
                peer.user_id != user_id && peer.is_included_in_peer_list()
            });

            // Make sure leech peer lists are filled with seeds
            if queries.left > 0 && torrent.seeders > 0 && queries.numwant > peers.len() {
                peers.extend(
                    valid_peers
                        .clone()
                        .filter(|(_index, peer)| peer.is_seeder)
                        .choose_multiple(&mut SmallRng::from_entropy(), queries.numwant),
                );
            }

            // Otherwise only send leeches until the numwant is reached
            if torrent.leechers > 0 && queries.numwant > peers.len() {
                peers.extend(
                    valid_peers
                        .filter(|(_index, peer)| !peer.is_seeder)
                        .choose_multiple(
                            &mut SmallRng::from_entropy(),
                            queries.numwant.saturating_sub(peers.len()),
                        ),
                );
            }

            // Split peers into ipv4 and ipv6 variants and serialize their socket
            // to bytes according to the bittorrent spec
            for (_index, peer) in peers.iter() {
                match peer.ip_address {
                    IpAddr::V4(ip) => {
                        peers_ipv4.extend(&ip.octets());
                        peers_ipv4.extend(&peer.port.to_be_bytes());
                    }
                    IpAddr::V6(ip) => {
                        peers_ipv6.extend(&ip.octets());
                        peers_ipv6.extend(&peer.port.to_be_bytes());
                    }
                }
            }
        }

        // Generate bencoded response to return to client

        let interval = SmallRng::from_entropy()
            .gen_range(tracker.config.announce_min..=tracker.config.announce_max);

        // Write out bencoded response (keys must be sorted to be within spec)
        let mut response: Vec<u8> = vec![];
        response.extend(b"d8:completei");
        response.extend(torrent.seeders.to_string().as_bytes());
        response.extend(b"e10:downloadedi");
        response.extend(torrent.times_completed.to_string().as_bytes());
        response.extend(b"e10:incompletei");
        response.extend(torrent.leechers.to_string().as_bytes());
        response.extend(b"e8:intervali");
        response.extend(interval.to_string().as_bytes());
        response.extend(b"e12:min intervali");
        response.extend(tracker.config.announce_min.to_string().as_bytes());
        response.extend(b"e5:peers");

        if peers_ipv4.is_empty() {
            response.extend(b"0:")
        } else {
            response.extend(peers_ipv4.len().to_string().as_bytes());
            response.extend(b":");
            response.extend(&peers_ipv4);
        }

        if !peers_ipv6.is_empty() {
            response.extend(b"6:peers6");
            response.extend(peers_ipv6.len().to_string().as_bytes());
            response.extend(b":");
            response.extend(peers_ipv6);
        }

        if let Some(warning) = warning_opt {
            response.extend(b"15:warning message");
            response.extend(warning.len().to_string().as_bytes());
            response.extend(b":");
            response.extend(warning.as_bytes());
        }

        response.extend(b"e");

        let upload_factor = std::cmp::max(
            tracker.config.upload_factor,
            std::cmp::max(group.upload_factor, torrent.upload_factor),
        );

        let download_factor = std::cmp::min(
            tracker.config.download_factor,
            std::cmp::min(group.download_factor, torrent.download_factor),
        );

        // Has to be dropped before any `await` calls.
        //
        // Unfortunately, `Drop` currently doesn't work in rust with borrowed values
        // so we have to use a giant scope instead.
        //
        // See:
        // - https://github.com/rust-lang/rust/issues/57478
        // - https://stackoverflow.com/questions/73519148/why-does-send-value-that-is-dropd-before-await-mean-the-future-is-send
        // - https://github.com/rust-lang/rust/issues/101135
        //    - Once this issue is fixed, we can remove the scope and rely solely on `Drop`.
        drop(torrent_guard);

        (
            upload_factor,
            download_factor,
            uploaded_delta,
            downloaded_delta,
            seeder_delta,
            leecher_delta,
            times_completed_delta,
            is_visible,
            user,
            user_id,
            group,
            response,
        )
    };

    let download_factor = if tracker
        .personal_freeleeches
        .read()
        .contains(&PersonalFreeleech { user_id })
        || tracker.freeleech_tokens.read().contains(&FreeleechToken {
            user_id,
            torrent_id,
        })
        || tracker
            .featured_torrents
            .read()
            .contains(&FeaturedTorrent { torrent_id })
        || user.is_donor
    {
        0
    } else {
        download_factor
    };

    let upload_factor = if tracker
        .featured_torrents
        .read()
        .contains(&FeaturedTorrent { torrent_id })
    {
        200
    } else {
        upload_factor
    };

    let credited_uploaded_delta = upload_factor as u64 * uploaded_delta / 100;
    let credited_downloaded_delta = download_factor as u64 * downloaded_delta / 100;

    let completed_at = if queries.event == Event::Completed {
        Some(Utc::now())
    } else {
        None
    };

    if seeder_delta != 0 || leecher_delta != 0 {
        tracker.users.write().entry(user_id).and_modify(|user| {
            user.num_seeding = user.num_seeding.saturating_add_signed(seeder_delta);
            user.num_leeching = user.num_leeching.saturating_add_signed(leecher_delta);
        });
    }

    let connectable = check_connectivity(&tracker, client_ip, queries.port).await;

    tracker.peer_updates.lock().upsert(
        queries.peer_id,
        client_ip,
        queries.port,
        String::from(user_agent),
        queries.uploaded,
        queries.downloaded,
        queries.event != Event::Stopped,
        queries.left == 0,
        is_visible,
        queries.left,
        torrent_id,
        user_id,
        connectable,
    );

    tracker.history_updates.lock().upsert(
        user_id,
        torrent_id,
        String::from(user_agent),
        credited_uploaded_delta,
        uploaded_delta,
        queries.uploaded,
        credited_downloaded_delta,
        downloaded_delta,
        queries.downloaded,
        queries.left == 0,
        queries.event != Event::Stopped,
        group.is_immune || user.is_donor,
        completed_at,
    );

    if credited_uploaded_delta != 0 || credited_downloaded_delta != 0 {
        tracker.user_updates.lock().upsert(
            user_id,
            credited_uploaded_delta,
            credited_downloaded_delta,
        );
    }

    if seeder_delta != 0 || leecher_delta != 0 || times_completed_delta != 0 {
        tracker.torrent_updates.lock().upsert(
            torrent_id,
            seeder_delta,
            leecher_delta,
            times_completed_delta,
        );
    }

    if tracker.config.is_announce_logging_enabled {
        tracker.announce_updates.lock().upsert(
            user_id,
            torrent_id,
            queries.uploaded,
            queries.downloaded,
            queries.left,
            queries.corrupt,
            queries.peer_id,
            queries.port,
            queries.numwant.try_into().unwrap_or(u16::MAX),
            queries.event,
            queries.key,
        );
    }

    tracker.stats.increment_announce_response();

    Ok(response)
}

async fn check_connectivity(tracker: &Arc<Tracker>, ip: IpAddr, port: u16) -> bool {
    if tracker.config.is_connectivity_check_enabled {
        let now = Utc::now();
        let socket = SocketAddr::from((ip, port));
        let connectable_port_opt = tracker.connectable_ports.read().get(&socket).cloned();

        if let Some(connectable_port) = connectable_port_opt {
            let ttl = Duration::seconds(tracker.config.connectivity_check_interval);

            if let Some(cached_until) = connectable_port.updated_at.checked_add_signed(ttl) {
                if cached_until > now {
                    return connectable_port.connectable;
                }
            }
        }

        let connectable = tokio::spawn(async move {
            tokio::time::timeout(
                std::time::Duration::from_millis(500),
                TcpStream::connect(socket),
            )
            .await
            .is_ok_and(|connection_result| connection_result.is_ok())
        })
        .await
        .unwrap_or(false);

        tracker
            .connectable_ports
            .write()
            .entry(socket)
            .and_modify(|connectable_port| {
                connectable_port.connectable = connectable;
                connectable_port.updated_at = now;
            })
            .or_insert(ConnectablePort {
                connectable,
                updated_at: now,
            });

        return connectable;
    }

    false
}
