use axum::response::IntoResponse;
use chrono::{DateTime, Utc};
use http::StatusCode;
use tokio::sync::{mpsc::error::SendError, oneshot};

#[derive(thiserror::Error, Debug)]
pub enum ChannelError {
    #[error("Send error")]
    Send(#[from] SendError<(String, oneshot::Sender<super::manager::ClientResult>)>),
    #[error("Receive error")]
    Receive(#[from] tokio::sync::oneshot::error::RecvError),
}

#[derive(thiserror::Error, Debug)]
pub enum StaticImageError {
    #[error("Must be a MD5 digest and image extension: {0}")]
    InvalidFormat(String),
    #[error("Must be a MD5 digest: {0}")]
    InvalidDigest(String),
    #[error("Must be a recognized image extension: {0}")]
    InvalidExtension(String),
    #[error("Image not found for digest: {0:x}")]
    ImageNotFound(md5::Digest),
    #[error("Error reading image for digest: {0:x}")]
    ImageIo(md5::Digest, std::io::Error),
}

impl IntoResponse for StaticImageError {
    fn into_response(self) -> axum::response::Response {
        match self {
            error @ (Self::InvalidFormat(_)
            | Self::InvalidDigest(_)
            | Self::InvalidExtension(_)
            | Self::ImageNotFound(_)) => {
                log::error!("{error}");
                (StatusCode::BAD_REQUEST, format!("{error}")).into_response()
            }
            ref error @ Self::ImageIo(_, ref io_error) => {
                log::error!("{error}: {io_error}");
                (StatusCode::INTERNAL_SERVER_ERROR, format!("{error}")).into_response()
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum RequestImageError {
    #[error("Must be a URL-safe Base64 string: {0}")]
    InvalidFormat(String),
    #[error("Must be valid UTF-8: {0:?}")]
    InvalidUtf8(Vec<u8>),
    #[error("Index database error")]
    Index(#[from] image_cache_index::db::Error),
    #[error("Image download previously failed ({1}): {0}")]
    DownloadFailed(String, DateTime<Utc>),
    #[error("Invalid image type: {0}")]
    InvalidImageType(image_cache::image_type::ImageType),
    #[error("Unexpected client status code: {0}")]
    UnexpectedStatus(StatusCode),
    #[error("Download queue error")]
    DownloadQueue(#[from] ChannelError),
    #[error("HTTP client error")]
    Http(#[from] image_cache::client::Error),
}

impl IntoResponse for RequestImageError {
    fn into_response(self) -> axum::response::Response {
        match self {
            error @ (Self::InvalidFormat(_)
            | Self::InvalidUtf8(_)
            | Self::DownloadFailed(_, _)
            | Self::InvalidImageType(_)) => {
                log::error!("{error}");
                (StatusCode::BAD_REQUEST, format!("{error}")).into_response()
            }
            ref error @ Self::Index(ref index_db_error) => {
                log::error!("{error}: {index_db_error}");

                (StatusCode::INTERNAL_SERVER_ERROR, format!("{error}")).into_response()
            }
            error @ Self::UnexpectedStatus(status_code) => {
                log::error!("{error}");
                (status_code, format!("{error}")).into_response()
            }
            ref error @ Self::DownloadQueue(ChannelError::Receive(ref receive_error)) => {
                log::error!("{error} (receive): {receive_error}");
                (StatusCode::INTERNAL_SERVER_ERROR, format!("{error}")).into_response()
            }
            ref error @ Self::DownloadQueue(ChannelError::Send(ref send_error)) => {
                log::error!("{error} (send): {send_error}");
                (StatusCode::INTERNAL_SERVER_ERROR, format!("{error}")).into_response()
            }
            ref error @ Self::Http(ref client_error) => {
                log::error!("{error}: {client_error}");

                (StatusCode::INTERNAL_SERVER_ERROR, format!("{error}")).into_response()
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum MapUrlsError {
    #[error("Index database error")]
    Index(#[from] image_cache_index::db::Error),
}

impl IntoResponse for MapUrlsError {
    fn into_response(self) -> axum::response::Response {
        match self {
            ref error @ Self::Index(ref index_db_error) => {
                log::error!("{error}: {index_db_error}");

                (StatusCode::INTERNAL_SERVER_ERROR, format!("{error}")).into_response()
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ShutdownError {
    #[error("Request task join error")]
    RequestTaskJoin(#[from] tokio::task::JoinError),
}
