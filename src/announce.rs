use axum::{
    async_trait, debug_handler,
    extract::{ConnectInfo, FromRef, FromRequestParts, Path, State},
    http::{
        header::{ACCEPT_CHARSET, ACCEPT_LANGUAGE, REFERER, USER_AGENT},
        request::Parts,
        HeaderMap,
    },
};
use peer::Peer;
use rand::{rngs::SmallRng, seq::IteratorRandom, Rng, SeedableRng};
use sqlx::types::chrono::Utc;
use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::Arc,
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
use crate::{error::Error, tracker::peer::UserAgent};

#[derive(Clone, Copy, PartialEq, Default)]
enum Event {
    Completed,
    #[default]
    Empty,
    Started,
    Stopped,
}

impl FromStr for Event {
    type Err = Error;

    fn from_str(event: &str) -> Result<Self, Error> {
        match event {
            "" | "empty" => Ok(Self::Empty),
            "completed" => Ok(Self::Completed),
            "started" => Ok(Self::Started),
            "stopped" => Ok(Self::Stopped),
            _ => Err(Error("Unsupported event type")),
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

pub struct Request<Announce>(pub Announce);

/// Extracts the query parameters in the HTTP GET request.
#[async_trait]
impl<S> FromRequestParts<S> for Request<Announce>
where
    S: Send + Sync,
    Arc<Tracker>: FromRef<S>,
{
    type Rejection = Error;

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
                .ok_or(Error("Invalid query string parameter."))?;
            let value = query_string
                .get(equal_sign_pos + 1..value_end_pos)
                .ok_or(Error("Invalid query string value."))?;

            match parameter {
                "info_hash" => {
                    info_hash = Some(InfoHash::from(utils::urlencoded_to_bytes(value).await?))
                }
                "peer_id" => peer_id = Some(PeerId::from(utils::urlencoded_to_bytes(value).await?)),
                "port" => {
                    port = Some(value.parse().map_err(|_| {
                        Error("Invalid 'port' (must be greater than or equal to 0).")
                    })?)
                }
                "uploaded" => {
                    uploaded = Some(value.parse().map_err(|_| {
                        Error("Invalid 'uploaded' (must be greater than or equal to 0).")
                    })?)
                }
                "downloaded" => {
                    downloaded = Some(value.parse().map_err(|_| {
                        Error("Invalid 'downloaded' (must be greater than or equal to 0).")
                    })?)
                }
                "left" => {
                    left = Some(value.parse().map_err(|_| {
                        Error("Invalid 'left' (must be greater than or equal to 0).")
                    })?)
                }
                "compact" => {
                    if value != "1" {
                        return Err(Error("Your client does not support compact announces."));
                    }
                }
                "event" => {
                    event = Some(
                        value
                            .parse()
                            .map_err(|_| Error("Unsupported 'event' type."))?,
                    )
                }
                "numwant" => {
                    numwant = Some(value.parse().map_err(|_| {
                        Error("Invalid 'numwant' (must be greater than or equal to 0).")
                    })?)
                }
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
            .map_err(|_| Error("Internal tracker error."))?;

        Ok(Request(Announce {
            info_hash: info_hash.ok_or(Error("Query parameter 'info_hash' is missing."))?,
            peer_id: peer_id.ok_or(Error("Query parameter 'peer_id' is missing."))?,
            port: port.ok_or(Error("Query parameter 'port' is missing."))?,
            uploaded: uploaded.ok_or(Error("Query parameter 'uploaded' is missing."))?,
            downloaded: downloaded.ok_or(Error("Query parameter 'downloaded' is missing."))?,
            left: left.ok_or(Error("Query parameter 'left' is missing."))?,
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

#[debug_handler]
pub async fn announce(
    State(tracker): State<Arc<Tracker>>,
    Path(passkey): Path<String>,
    Request(queries): Request<Announce>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> Result<Vec<u8>, Error> {
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
        return Err(Error("Abnormal access blocked."));
    }

    // User agent header is required.
    let user_agent = headers
        .get(USER_AGENT)
        .ok_or(Error("Invalid user agent."))?
        .to_str()
        .map_err(|_| Error("Invalid user agent."))?;

    // Block user agent strings that are too long. (For Database reasons)
    if user_agent.len() > 64 {
        return Err(Error("The user agent of this client is too long."));
    }

    // Block user agent strings on the blacklist
    if tracker.agent_blacklist.read().await.contains(&Agent {
        name: user_agent.to_string(),
    }) {
        return Err(Error(
            "Client is not acceptable. Please check our blacklist.",
        ));
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
        return Err(Error("Browser, crawler or cheater is not allowed."));
    }

    let passkey: Passkey = Passkey::from_str(&passkey).map_err(|_| Error("Invalid passkey."))?;

    // Validate passkey
    let user_guard = tracker.passkey2id.read().await;
    let user_id = user_guard.get(&passkey).ok_or(Error(
        "Passkey does not exist. Please re-download the .torrent file.",
    ))?;
    let mut user_guard = tracker.users.write().await;
    let mut user = user_guard.get_mut(user_id).ok_or(Error(
        "User does not exist. Please re-download the .torrent file.",
    ))?;

    // Validate user
    if !user.can_download && queries.left != 0 {
        return Err(Error("Your downloading privileges have been disabled."));
    }

    // Validate port
    // Some clients send port 0 on the stopped event
    if tracker.port_blacklist.read().await.contains(&queries.port)
        && queries.event != Event::Stopped
    {
        return Err(Error("Illegal port. Port should be between 6881-64999."));
    }

    // Validate torrent
    let torrent_guard = tracker.infohash2id.read().await;
    let torrent_id = torrent_guard
        .get(&queries.info_hash)
        .ok_or(Error("Infohash not found."))?
        .to_owned();
    let mut torrent_guard = tracker.torrents.write().await;
    let mut torrent = torrent_guard
        .get_mut(&torrent_id)
        .ok_or(Error("Torrent not found."))?;

    if torrent.is_deleted {
        return Err(Error("Torrent has been deleted."));
    }

    if torrent.status != tracker::torrent::Status::Approved {
        match torrent.status {
            tracker::torrent::Status::Pending => {
                return Err(Error("Torrent is pending moderation."))
            }
            tracker::torrent::Status::Rejected => return Err(Error("Torrent has been rejected.")),
            tracker::torrent::Status::Postponed => {
                return Err(Error("Torrent has been postponed."))
            }
            _ => return Err(Error("Torrent not approved.")),
        }
    }

    // Make sure user isn't leeching more torrents than their group allows
    if queries.left > 0 && matches!(user.download_slots, Some(slots) if slots > user.num_leeching) {
        return Err(Error("Your download slot limit is reached."));
    }

    // Change of upload/download compared to previous announce
    let uploaded_delta;
    let downloaded_delta;

    if queries.event == Event::Stopped {
        // Try and remove the peer
        let removed_peer = torrent.peers.write().await.remove(&tracker::peer::Index {
            user_id: user.id,
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
                    user.num_seeding = user.num_seeding.saturating_sub(1);
                    torrent.seeders = torrent.seeders.saturating_sub(1);
                } else {
                    user.num_leeching = user.num_leeching.saturating_sub(1);
                    torrent.leechers = torrent.leechers.saturating_sub(1);
                }
                // Schedule a torrent update in the mysql db
                tracker.torrent_updates.write().await.upsert(
                    torrent.id,
                    torrent.seeders,
                    torrent.leechers,
                    torrent.times_completed,
                );
            }
            // Schedule a peer deletion in the mysql db
            tracker
                .peer_deletions
                .write()
                .await
                .upsert(torrent.id, user.id, queries.peer_id);
        } else {
            return Err(Error("Stopped torrent doesn't exist."));
        }
    } else {
        // Schedule a peer update in the mysql db
        tracker.peer_updates.write().await.upsert(
            queries.peer_id,
            addr.ip(),
            queries.port,
            UserAgent::from_str(user_agent).unwrap(),
            queries.uploaded,
            queries.downloaded,
            queries.left == 0,
            queries.left,
            torrent.id,
            user.id,
        );

        // Insert the peer into the in-memory db
        let old_peer = torrent.peers.write().await.insert(
            tracker::peer::Index {
                user_id: user.id,
                peer_id: queries.peer_id,
            },
            tracker::Peer {
                ip_address: addr.ip(),
                user_id: user.id,
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
        let update_peer_counts: bool;
        match old_peer {
            Some(old_peer) => {
                if queries.left == 0 && !old_peer.is_seeder {
                    // leech has turned into a seed
                    user.num_seeding += 1;
                    torrent.seeders += 1;
                    torrent.times_completed += 1;

                    if old_peer.is_active {
                        user.num_leeching = user.num_leeching.saturating_sub(1);
                        torrent.leechers = torrent.leechers.saturating_sub(1);
                    }
                    update_peer_counts = true;
                } else if queries.left > 0 && old_peer.is_seeder {
                    // seed has turned into a leech
                    user.num_leeching += 1;
                    torrent.leechers += 1;

                    if old_peer.is_active {
                        user.num_seeding = user.num_seeding.saturating_sub(1);
                        torrent.seeders = torrent.seeders.saturating_sub(1);
                    }
                    update_peer_counts = true;
                } else {
                    if old_peer.is_active {
                        update_peer_counts = false;
                    } else {
                        update_peer_counts = true;
                        if queries.left == 0 {
                            // seeder is reactivated
                            user.num_seeding += 1;
                            torrent.seeders += 1;
                        } else {
                            // leecher is reactivated
                            user.num_leeching += 1;
                            torrent.leechers += 1;
                        }
                    }
                }

                // Calculate change in upload and download compared to previous
                // announce
                uploaded_delta = queries.uploaded.saturating_sub(old_peer.uploaded);
                downloaded_delta = queries.downloaded.saturating_sub(old_peer.downloaded);
            }
            None => {
                // new peer is inserted
                update_peer_counts = true;
                if queries.left == 0 {
                    // new seeder is inserted
                    user.num_seeding += 1;
                    torrent.seeders += 1;
                } else {
                    // new leecher is inserted
                    user.num_leeching += 1;
                    torrent.leechers += 1;
                }

                // Calculate change in upload and download compared to previous
                // announce
                uploaded_delta = queries.uploaded;
                downloaded_delta = queries.downloaded;
            }
        }
        if update_peer_counts {
            // Schedule a torrent update in the mysql db
            tracker.torrent_updates.write().await.upsert(
                torrent.id,
                torrent.seeders,
                torrent.leechers,
                torrent.times_completed,
            );
        }
    }

    let download_factor = if tracker
        .personal_freeleeches
        .read()
        .await
        .contains(&PersonalFreeleech { user_id: user.id })
        || tracker
            .freeleech_tokens
            .read()
            .await
            .contains(&FreeleechToken {
                user_id: user.id,
                torrent_id: torrent.id,
            }) {
        0
    } else {
        std::cmp::min(
            tracker.config.download_factor,
            std::cmp::min(user.download_factor, torrent.download_factor),
        )
    };

    let upload_factor = std::cmp::max(
        tracker.config.upload_factor,
        std::cmp::max(user.upload_factor, torrent.upload_factor),
    );

    let credited_uploaded_delta = upload_factor as u64 * uploaded_delta / 100;
    let credited_downloaded_delta = download_factor as u64 * downloaded_delta / 100;

    tracker.history_updates.write().await.upsert(
        user.id,
        torrent.id,
        UserAgent::from_str(user_agent).unwrap(),
        credited_uploaded_delta,
        uploaded_delta,
        queries.uploaded,
        credited_downloaded_delta,
        downloaded_delta,
        queries.downloaded,
        queries.left != 0,
        queries.event != Event::Stopped,
        user.is_immune,
    );

    tracker.user_updates.write().await.upsert(
        user.id,
        credited_uploaded_delta,
        credited_downloaded_delta,
    );

    let mut peer_list: Vec<(&peer::Index, &Peer)> = Vec::new();

    // Don't return peers with the same user id or those that are marked as inactive
    let peers_guard = torrent.peers.read().await;
    let valid_peers = peers_guard
        .iter()
        .filter(|(_index, peer)| peer.user_id != user.id && peer.is_active);

    // Make sure leech peerlists are filled with seeds
    if queries.left > 0 && torrent.seeders > 0 {
        peer_list.extend(
            valid_peers
                .clone()
                .filter(|(_index, peer)| peer.is_seeder)
                .choose_multiple(&mut SmallRng::from_entropy(), queries.numwant),
        );
    }
    // Otherwise only send leeches until the numwant is reached
    if torrent.leechers > 0 {
        peer_list.extend(
            valid_peers
                .clone()
                .filter(|(_index, peer)| !peer.is_seeder)
                .choose_multiple(
                    &mut SmallRng::from_entropy(),
                    queries.numwant.saturating_sub(peer_list.len()),
                ),
        );
    }

    let mut peers: Vec<u8> = vec![];
    let mut peers6: Vec<u8> = vec![];

    for (_index, peer) in peer_list.iter() {
        match peer.ip_address {
            IpAddr::V4(ip) => {
                peers.extend(&ip.octets());
                peers.extend(&peer.port.to_be_bytes());
            }
            IpAddr::V6(ip) => {
                peers6.extend(&ip.octets());
                peers6.extend(&peer.port.to_be_bytes());
            }
        }
    }

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
    response.extend(peers.len().to_string().as_bytes());
    response.extend(b":");
    response.extend(&peers);

    if !peers6.is_empty() {
        response.extend(b"e6:peers6");
        response.extend(peers6.len().to_string().as_bytes());
        response.extend(b":");
        response.extend(peers6);
    }

    response.extend(b"e");

    Ok(response)
}
