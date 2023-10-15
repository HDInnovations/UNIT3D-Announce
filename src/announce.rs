use axum::{
    async_trait,
    extract::{ConnectInfo, FromRef, FromRequestParts, Path, State},
    http::{
        header::{ACCEPT_CHARSET, ACCEPT_LANGUAGE, REFERER, USER_AGENT},
        request::Parts,
        HeaderMap,
    },
};
use compact_str::CompactString;
use peer::Peer;
use rand::{rngs::SmallRng, seq::IteratorRandom, Rng, SeedableRng};
use sqlx::types::chrono::Utc;
use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::Arc,
};

use crate::error::AnnounceError::{
    self, AbnormalAccess, BlacklistedClient, BlacklistedPort, DownloadPrivilegesRevoked,
    DownloadSlotLimitReached, InfoHashNotFound, InternalTrackerError, InvalidCompact,
    InvalidDownloaded, InvalidInfoHash, InvalidLeft, InvalidNumwant, InvalidPasskey, InvalidPeerId,
    InvalidPort, InvalidQueryStringKey, InvalidQueryStringValue, InvalidUploaded, InvalidUserAgent,
    MissingDownloaded, MissingInfoHash, MissingLeft, MissingPeerId, MissingPort, MissingUploaded,
    NotAClient, PasskeyNotFound, StoppedPeerDoesntExist, TorrentIsDeleted,
    TorrentIsPendingModeration, TorrentIsPostponed, TorrentIsRejected, TorrentNotFound,
    TorrentUnknownModerationStatus, UnsupportedEvent, UserAgentTooLong, UserNotFound,
};

use crate::tracker::{
    self,
    blacklisted_agent::Agent,
    freeleech_token::FreeleechToken,
    peer::{self, PeerId},
    personal_freeleech::PersonalFreeleech,
    torrent::InfoHash,
    user::Passkey,
    Tracker,
};
use crate::utils;

#[derive(Clone, Copy, PartialEq, Default)]
enum Event {
    Completed,
    #[default]
    Empty,
    Started,
    Stopped,
}

impl FromStr for Event {
    type Err = AnnounceError;

    fn from_str(event: &str) -> Result<Self, AnnounceError> {
        match event {
            "" | "empty" => Ok(Self::Empty),
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
        }))
    }
}

pub struct ClientIp(pub std::net::IpAddr);

#[async_trait]
impl<S> FromRequestParts<S> for ClientIp
where
    S: Send + Sync,
{
    type Rejection = AnnounceError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        // Extract the IP from the X-Real-IP header set by nginx using real_ip_recursive
        if let Some(ip_header) = parts.headers.get("X-Real-IP") {
            if let Ok(ip_str) = ip_header.to_str() {
                if let Ok(ip) = IpAddr::from_str(ip_str) {
                    return Ok(ClientIp(ip));
                }
            }
        }

        // If the X-Real-IP header isn't included, or if parsing the ip from it fails, then use the
        // connecting ip.
        let ConnectInfo(addr) = ConnectInfo::<SocketAddr>::from_request_parts(parts, state)
            .await
            .or(Err(InternalTrackerError))?;

        Ok(ClientIp(addr.ip()))
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
    if tracker.agent_blacklist.read().await.contains(&Agent {
        name: user_agent.to_string(),
    }) {
        return Err(BlacklistedClient);
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
    if tracker.port_blacklist.read().await.contains(&queries.port)
        && queries.event != Event::Stopped
    {
        return Err(BlacklistedPort);
    }

    let passkey: Passkey = Passkey::from_str(&passkey).or(Err(InvalidPasskey))?;

    // Validate passkey
    let user_id = tracker
        .passkey2id
        .read()
        .await
        .get(&passkey)
        .ok_or(PasskeyNotFound)?
        .clone();
    let user = tracker
        .users
        .read()
        .await
        .get(&user_id)
        .ok_or(UserNotFound)?
        .clone();

    // Validate user
    if !user.can_download && queries.left != 0 {
        return Err(DownloadPrivilegesRevoked);
    }

    // Validate torrent
    let torrent_guard = tracker.infohash2id.read().await;
    let torrent_id = torrent_guard
        .get(&queries.info_hash)
        .ok_or(InfoHashNotFound)?
        .to_owned();
    let mut torrent_guard = tracker.torrents.write().await;
    let mut torrent = torrent_guard.get_mut(&torrent_id).ok_or(TorrentNotFound)?;

    if torrent.is_deleted {
        return Err(TorrentIsDeleted);
    }

    if torrent.status != tracker::torrent::Status::Approved {
        match torrent.status {
            tracker::torrent::Status::Pending => return Err(TorrentIsPendingModeration),
            tracker::torrent::Status::Rejected => return Err(TorrentIsRejected),
            tracker::torrent::Status::Postponed => return Err(TorrentIsPostponed),
            _ => return Err(TorrentUnknownModerationStatus),
        }
    }

    // Make sure user isn't leeching more torrents than their group allows
    if queries.left > 0 && matches!(user.download_slots, Some(slots) if slots <= user.num_leeching)
    {
        return Err(DownloadSlotLimitReached);
    }

    // Change of upload/download compared to previous announce
    let uploaded_delta;
    let downloaded_delta;
    let seeder_delta;
    let leecher_delta;
    let times_completed_delta;

    if queries.event == Event::Stopped {
        // Try and remove the peer
        let removed_peer = torrent.peers.remove(&tracker::peer::Index {
            user_id: user_id,
            peer_id: queries.peer_id,
        });
        // Check if peer was removed
        if let Some(peer) = removed_peer {
            // Calculate change in upload and download compared to previous
            // announce
            uploaded_delta = queries.uploaded.saturating_sub(peer.uploaded);
            downloaded_delta = queries.downloaded.saturating_sub(peer.downloaded);

            if peer.is_active {
                if peer.is_seeder {
                    seeder_delta = -1;
                    leecher_delta = 0;
                } else {
                    seeder_delta = 0;
                    leecher_delta = -1;
                }
            } else {
                seeder_delta = 0;
                leecher_delta = 0;
            }
        } else {
            return Err(StoppedPeerDoesntExist);
        }

        times_completed_delta = 0;
    } else {
        // Insert the peer into the in-memory db
        let old_peer = torrent.peers.insert(
            tracker::peer::Index {
                user_id: user_id,
                peer_id: queries.peer_id,
            },
            tracker::Peer {
                ip_address: client_ip,
                user_id: user_id,
                torrent_id: torrent.id,
                port: queries.port,
                is_seeder: queries.left == 0,
                is_active: true,
                updated_at: Utc::now(),
                uploaded: queries.uploaded,
                downloaded: queries.downloaded,
            },
        );

        // Update the user and torrent seeding/leeching counts in the
        // in-memory db
        match old_peer {
            Some(old_peer) => {
                if queries.left == 0 && !old_peer.is_seeder {
                    // leech has turned into a seed
                    seeder_delta = 1;
                    times_completed_delta = 1;

                    if old_peer.is_active {
                        leecher_delta = -1;
                    } else {
                        leecher_delta = 0;
                    }
                } else if queries.left > 0 && old_peer.is_seeder {
                    // seed has turned into a leech
                    leecher_delta = 1;
                    times_completed_delta = 0;

                    if old_peer.is_active {
                        seeder_delta = -1;
                    } else {
                        seeder_delta = 0;
                    }
                } else {
                    times_completed_delta = 0;

                    if !old_peer.is_active {
                        if queries.left == 0 {
                            // seeder is reactivated
                            seeder_delta = 1;
                            leecher_delta = 0;
                        } else {
                            // leecher is reactivated
                            seeder_delta = 0;
                            leecher_delta = 1;
                        }
                    } else {
                        seeder_delta = 0;
                        leecher_delta = 0;
                    }
                }

                // Calculate change in upload and download compared to previous
                // announce
                uploaded_delta = queries.uploaded.saturating_sub(old_peer.uploaded);
                downloaded_delta = queries.downloaded.saturating_sub(old_peer.downloaded);
            }
            None => {
                // new peer is inserted
                if queries.left == 0 {
                    // new seeder is inserted
                    leecher_delta = 0;
                    seeder_delta = 1;
                } else {
                    // new leecher is inserted
                    seeder_delta = 0;
                    leecher_delta = 1;
                }

                times_completed_delta = 0;

                // Calculate change in upload and download compared to previous
                // announce
                uploaded_delta = queries.uploaded;
                downloaded_delta = queries.downloaded;
            }
        }
    }

    torrent.seeders = torrent.seeders.saturating_add_signed(seeder_delta);
    torrent.leechers = torrent.leechers.saturating_add_signed(leecher_delta);

    // Generate peer lists to return to client

    let mut peers_ipv4: Vec<u8> = Vec::new();
    let mut peers_ipv6: Vec<u8> = Vec::new();

    if queries.event != Event::Stopped && (torrent.leechers != 0 || queries.left != 0) {
        let mut peers: Vec<(&peer::Index, &Peer)> = Vec::new();

        // Don't return peers with the same user id or those that are marked as inactive
        let valid_peers = torrent
            .peers
            .iter()
            .filter(|(_index, peer)| peer.user_id != user_id && peer.is_active);

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
    response.extend(peers_ipv4.len().to_string().as_bytes());
    response.extend(b":");
    response.extend(&peers_ipv4);

    if !peers_ipv6.is_empty() {
        response.extend(b"e6:peers6");
        response.extend(peers_ipv6.len().to_string().as_bytes());
        response.extend(b":");
        response.extend(peers_ipv6);
    }

    response.extend(b"e");

    let upload_factor = std::cmp::max(
        tracker.config.upload_factor,
        std::cmp::max(user.upload_factor, torrent.upload_factor),
    );

    let download_factor = std::cmp::min(
        tracker.config.download_factor,
        std::cmp::min(user.download_factor, torrent.download_factor),
    );

    let download_factor = if tracker
        .personal_freeleeches
        .read()
        .await
        .contains(&PersonalFreeleech { user_id })
        || tracker
            .freeleech_tokens
            .read()
            .await
            .contains(&FreeleechToken {
                user_id,
                torrent_id: torrent.id,
            }) {
        0
    } else {
        download_factor
    };

    let credited_uploaded_delta = upload_factor as u64 * uploaded_delta / 100;
    let credited_downloaded_delta = download_factor as u64 * downloaded_delta / 100;

    let completed_at = if queries.event == Event::Completed {
        Some(Utc::now())
    } else {
        None
    };

    if seeder_delta != 0 || leecher_delta != 0 {
        tracker
            .users
            .write()
            .await
            .entry(user_id)
            .and_modify(|user| {
                user.num_seeding = user.num_seeding.saturating_add_signed(seeder_delta);
                user.num_leeching = user.num_leeching.saturating_add_signed(leecher_delta);
            });
    }

    tracker.peer_updates.write().await.upsert(
        queries.peer_id,
        client_ip,
        queries.port,
        CompactString::from(user_agent),
        queries.uploaded,
        queries.downloaded,
        queries.event != Event::Stopped,
        queries.left == 0,
        queries.left,
        torrent.id,
        user_id,
    );

    tracker.history_updates.write().await.upsert(
        user_id,
        torrent.id,
        CompactString::from(user_agent),
        credited_uploaded_delta,
        uploaded_delta,
        queries.uploaded,
        credited_downloaded_delta,
        downloaded_delta,
        queries.downloaded,
        queries.left == 0,
        queries.event != Event::Stopped,
        user.is_immune,
        completed_at,
    );

    tracker.user_updates.write().await.upsert(
        user_id,
        credited_uploaded_delta,
        credited_downloaded_delta,
    );

    if seeder_delta != 0 || leecher_delta != 0 || times_completed_delta != 0 {
        tracker.torrent_updates.write().await.upsert(
            torrent_id,
            seeder_delta,
            leecher_delta,
            times_completed_delta,
        );
    }

    Ok(response)
}
