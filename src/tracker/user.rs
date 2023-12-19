use std::ops::DerefMut;
use std::str::FromStr;
use std::{ops::Deref, sync::Arc};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use sqlx::MySqlPool;

use anyhow::{Context, Result};

use crate::tracker::Tracker;

pub mod passkey;
pub use passkey::Passkey;

pub mod passkey2id;

#[derive(Serialize)]
pub struct Map(IndexMap<u32, User>);

impl Map {
    pub fn new() -> Map {
        Map(IndexMap::new())
    }

    pub async fn from_db(db: &MySqlPool) -> Result<Map> {
        let users = sqlx::query_as!(
            User,
            r#"
                SELECT
                    users.id as `id: u32`,
                    users.group_id as `group_id: i32`,
                    users.passkey as `passkey: Passkey`,
                    users.can_download as `can_download: bool`,
                    CAST(COALESCE(SUM(peers.seeder = 1 AND peers.active = 1 AND peers.visible = 1), 0) AS UNSIGNED) as `num_seeding: u32`,
                    CAST(COALESCE(SUM(peers.seeder = 0 AND peers.active = 1 AND peers.visible = 1), 0) AS UNSIGNED) as `num_leeching: u32`,
                    users.is_donor as `is_donor: bool`,
                    users.is_lifetime as `is_lifetime: bool`
                FROM
                    users
                LEFT JOIN
                    peers
                ON
                    users.id = peers.user_id
                WHERE
                    users.deleted_at IS NULL
                GROUP BY
                    users.id
            "#
        )
        .fetch_all(db)
        .await
        .context("Failed loading users.")?;

        let mut user_map = Map::new();

        for user in users {
            user_map.insert(user.id, user);
        }
        Ok(user_map)
    }

    pub async fn upsert(
        State(tracker): State<Arc<Tracker>>,
        Json(user): Json<APIInsertUser>,
    ) -> StatusCode {
        println!("Received user: {}", user.id);
        if let Ok(passkey) = Passkey::from_str(&user.passkey) {
            println!("Inserting user with id {}.", user.id);

            tracker.users.write().insert(
                user.id,
                User {
                    id: user.id,
                    group_id: user.group_id,
                    passkey,
                    can_download: user.can_download,
                    num_seeding: user.num_seeding,
                    num_leeching: user.num_leeching,
                    is_donor: user.is_donor,
                    is_lifetime: user.is_lifetime,
                },
            );

            tracker.passkey2id.write().insert(passkey, user.id);

            return StatusCode::OK;
        }

        StatusCode::BAD_REQUEST
    }

    pub async fn destroy(
        State(tracker): State<Arc<Tracker>>,
        Json(user): Json<APIRemoveUser>,
    ) -> StatusCode {
        if let Ok(passkey) = Passkey::from_str(&user.passkey) {
            println!("Removing user with id {}.", user.id);

            tracker.users.write().swap_remove(&user.id);
            tracker.passkey2id.write().swap_remove(&passkey);

            return StatusCode::OK;
        }

        StatusCode::BAD_REQUEST
    }

    pub async fn show(
        State(tracker): State<Arc<Tracker>>,
        Path(id): Path<u32>,
    ) -> Result<Json<User>, StatusCode> {
        tracker
            .users
            .read()
            .get(&id)
            .map(|user| Json(user.clone()))
            .ok_or(StatusCode::NOT_FOUND)
    }
}

impl Deref for Map {
    type Target = IndexMap<u32, User>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Map {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Clone, Deserialize, Hash, Serialize)]
pub struct User {
    pub id: u32,
    pub group_id: i32,
    pub passkey: Passkey,
    pub can_download: bool,
    pub num_seeding: u32,
    pub num_leeching: u32,
    pub is_donor: bool,
    pub is_lifetime: bool,
}

#[derive(Clone, Deserialize, Hash)]
pub struct APIInsertUser {
    pub id: u32,
    pub group_id: i32,
    pub passkey: String,
    pub can_download: bool,
    pub num_seeding: u32,
    pub num_leeching: u32,
    pub is_donor: bool,
    pub is_lifetime: bool,
}

#[derive(Clone, Deserialize, Hash)]
pub struct APIRemoveUser {
    pub id: u32,
    pub passkey: String,
}
