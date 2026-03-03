use chrono::{DateTime, Utc};
use image_cache::image_type::ImageType;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct DownloadLogEntry {
    pub status: DownloadStatus,
    #[serde(with = "chrono::serde::ts_seconds")]
    pub timestamp: DateTime<Utc>,
    #[serde(with = "serde_hex::SerHex::<serde_hex::config::Strict>")]
    pub digest: [u8; 16],
    pub image_type: ImageType,
    pub url: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum DownloadStatus {
    #[serde(rename = "A")]
    Added,
    #[serde(rename = "F")]
    Found,
}
