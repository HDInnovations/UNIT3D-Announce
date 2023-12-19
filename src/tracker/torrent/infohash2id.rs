use std::ops::{Deref, DerefMut};

use crate::tracker::torrent::InfoHash;
use indexmap::IndexMap;
use sqlx::MySqlPool;

use anyhow::{Context, Result};

pub struct Map(IndexMap<InfoHash, u32>);

impl Deref for Map {
    type Target = IndexMap<InfoHash, u32>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Map {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Map {
    pub fn new() -> Map {
        Map(IndexMap::new())
    }

    pub async fn from_db(db: &MySqlPool) -> Result<Map> {
        let info_hash2ids = sqlx::query_as!(
            InfoHash2Id,
            r#"
                SELECT
                    torrents.id as `id: u32`,
                    torrents.info_hash as `info_hash: InfoHash`
                FROM
                    torrents
                WHERE
                    torrents.deleted_at IS NULL
            "#
        )
        .fetch_all(db)
        .await
        .context("Failed loading torrent infohash to id mappings.")?;

        let mut info_hash2id_map = Map::new();

        for info_hash2id in info_hash2ids {
            info_hash2id_map.insert(info_hash2id.info_hash, info_hash2id.id);
        }

        Ok(info_hash2id_map)
    }
}

pub struct InfoHash2Id {
    pub id: u32,
    pub info_hash: InfoHash,
}
