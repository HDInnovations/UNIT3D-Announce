use std::{
    cmp::min,
    ops::{Deref, DerefMut},
};

use chrono::Utc;
use indexmap::IndexMap;
use sqlx::{MySql, MySqlPool, QueryBuilder};

pub struct Queue(pub IndexMap<Index, TorrentUpdate>);

#[derive(Eq, Hash, PartialEq)]
pub struct Index {
    pub torrent_id: u32,
}

#[derive(Clone, Copy)]
pub struct TorrentUpdate {
    pub torrent_id: u32,
    pub seeders: u32,
    pub leechers: u32,
    pub times_completed: u32,
}

impl Queue {
    pub fn new() -> Queue {
        Queue(IndexMap::new())
    }

    pub fn upsert(&mut self, torrent_id: u32, seeders: u32, leechers: u32, times_completed: u32) {
        self.insert(
            Index { torrent_id },
            TorrentUpdate {
                torrent_id,
                seeders,
                leechers,
                times_completed,
            },
        );
    }

    /// Flushes torrent updates to the mysql db
    pub async fn flush_to_db(&mut self, db: &MySqlPool) {
        let len = self.len();

        if len == 0 {
            return;
        }

        const BIND_LIMIT: usize = 65535;
        const NUM_TORRENT_COLUMNS: usize = 18;
        const TORRENT_LIMIT: usize = BIND_LIMIT / NUM_TORRENT_COLUMNS;

        let mut torrent_updates: Vec<TorrentUpdate> = vec![];

        torrent_updates.extend(self.split_off(len - min(TORRENT_LIMIT, len)).values());

        let now = Utc::now();

        // Trailing space required before the push values function
        // Leading space required after the push values function
        let mut query_builder: QueryBuilder<MySql> = QueryBuilder::new(
            r#"
                INSERT INTO
                    torrents(
                        id,
                        name,
                        description,
                        info_hash,
                        file_name,
                        num_file,
                        size,
                        seeders,
                        leechers,
                        times_completed,
                        announce,
                        user_id,
                        created_at,
                        updated_at,
                        type_id,
                        balance,
                        balance_offset
                    )
            "#,
        );

        query_builder
            .push_values(torrent_updates.clone(), |mut bind, torrent_update| {
                bind.push_bind(torrent_update.torrent_id)
                    .push_bind("")
                    .push_bind("")
                    .push_bind("")
                    .push_bind("")
                    .push_bind(0)
                    .push_bind(0)
                    .push_bind(torrent_update.seeders)
                    .push_bind(torrent_update.leechers)
                    .push_bind(torrent_update.times_completed)
                    .push_bind("")
                    .push_bind(1)
                    .push_bind(now)
                    .push_bind(now)
                    .push_bind(0)
                    .push_bind(0)
                    .push_bind(0);
            })
            .push(
                r#"
                    ON DUPLICATE KEY UPDATE
                        seeders = VALUES(seeders),
                        leechers = VALUES(leechers),
                        times_completed = VALUES(times_completed),
                        updated_at = VALUES(updated_at)
                "#,
            );

        let result = query_builder
            .build()
            .persistent(false)
            .execute(db)
            .await
            .map(|result| result.rows_affected());

        match result {
            Ok(_) => (),
            Err(e) => {
                println!("Torrent update failed: {}", e);
                torrent_updates.into_iter().for_each(|torrent_update| {
                    self.upsert(
                        torrent_update.torrent_id,
                        torrent_update.seeders,
                        torrent_update.leechers,
                        torrent_update.times_completed,
                    );
                })
            }
        }
    }
}

impl Deref for Queue {
    type Target = IndexMap<Index, TorrentUpdate>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Queue {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
