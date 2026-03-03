#![warn(clippy::all, clippy::pedantic, clippy::nursery, rust_2018_idioms)]
#![allow(clippy::missing_errors_doc)]
#![forbid(unsafe_code)]
use crate::manager::Manager;
use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Query, State},
    response::{IntoResponse, Redirect, Response},
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::Utc;
use clap::Parser;
use image_cache::image_type::ImageType;
use image_cache::store::{PrefixPartLengths, Store};
use image_cache_index::Entry;
use std::sync::Arc;
use std::{path::PathBuf, time::Duration};
use tokio_util::io::ReaderStream;

mod error;
mod manager;
mod shutdown;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();

    match opts.command {
        Command::Serve {
            base,
            server,
            store,
            prefix,
            index,
            buffer,
            delay,
        } => {
            tracing_subscriber::fmt()
                .with_max_level(opts.verbosity)
                .init();

            let store = Store::new(store).with_prefix_part_lengths(prefix.0)?;
            let manager = Arc::new(Manager::new(
                manager::UrlConfig::new(false, server.clone(), base.clone()),
                store,
                index,
                buffer,
                Duration::from_millis(delay),
            )?);

            let static_path = format!("{base}static/{{digest_with_image_type}}");
            let request_path = format!("{base}request/{{url}}");
            let urls_path = format!("{base}urls");

            let app = Router::new()
                .route(
                    &static_path,
                    get(|manager, digest_with_image_type| {
                        static_image(manager, digest_with_image_type)
                    }),
                )
                .with_state(manager.clone())
                .route(&request_path, get(request_image))
                .with_state(manager.clone())
                .route(&urls_path, post(map_urls))
                .with_state(manager.clone())
                .layer(tower_http::trace::TraceLayer::new_for_http());

            let listener = tokio::net::TcpListener::bind(server).await.unwrap();

            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown::signal(manager))
                .await
                .unwrap();
        }
    }

    Ok(())
}

async fn static_image(
    State(manager): State<Arc<Manager>>,
    Path(digest_with_image_type): Path<String>,
) -> Result<Response, error::StaticImageError> {
    let parts = digest_with_image_type.split('.').collect::<Vec<_>>();

    if parts.len() == 2 {
        let digest_bytes: [u8; 16] = hex::FromHex::from_hex(parts[0])
            .map_err(|_| error::StaticImageError::InvalidDigest(parts[0].to_string()))?;

        let digest = md5::Digest(digest_bytes);

        let image_mime_type = parts[1]
            .parse::<ImageType>()
            .ok()
            .and_then(image_cache::image_type::ImageType::mime_type)
            .ok_or_else(|| error::StaticImageError::InvalidExtension(parts[1].to_string()))?;

        let path = manager
            .path_for_digest(md5::Digest(digest_bytes))
            .ok_or(error::StaticImageError::ImageNotFound(digest))?;

        let headers = [(http::header::CONTENT_TYPE, image_mime_type.essence_str())];

        let body = tokio::fs::File::open(path)
            .await
            .map(|file| Body::from_stream(ReaderStream::new(file)))
            .map_err(|error| error::StaticImageError::ImageIo(digest, error))?;

        Ok((headers, body).into_response())
    } else {
        Err(error::StaticImageError::InvalidFormat(
            digest_with_image_type,
        ))
    }
}

async fn request_image(
    State(manager): State<Arc<Manager>>,
    Path(url): Path<String>,
) -> Result<Response, error::RequestImageError> {
    let url_bytes = URL_SAFE_NO_PAD
        .decode(&url)
        .map_err(|_| error::RequestImageError::InvalidFormat(url))?;

    let url = std::str::from_utf8(&url_bytes)
        .map_err(|_| error::RequestImageError::InvalidUtf8(url_bytes.clone()))?;

    match manager
        .lookup_status(url)
        .map_err(error::RequestImageError::from)?
    {
        manager::ImageStatus::Downloaded { entry } => Ok(Redirect::permanent(&manager.static_url(
            entry.digest,
            entry.image_type.into(),
            manager::UrlStyle::Absolute,
        ))
        .into_response()),
        manager::ImageStatus::Downloading => {
            let (bytes, action) = manager
                .request(url)
                .await
                .map_err(error::RequestImageError::from)?
                .map_err(error::RequestImageError::from)?
                .map_err(error::RequestImageError::UnexpectedStatus)?;

            match action.image_type.mime_type().zip(action.image_type.value()) {
                Some((mime_type, image_type)) => {
                    let headers = [(http::header::CONTENT_TYPE, mime_type.essence_str())];

                    manager
                        .index
                        .add(
                            url,
                            Entry {
                                timestamp: Utc::now(),
                                digest: action.entry.digest,
                                image_type,
                            },
                        )
                        .map_err(error::RequestImageError::from)?;

                    Ok((headers, bytes).into_response())
                }
                None => Err(error::RequestImageError::InvalidImageType(
                    action.image_type,
                )),
            }
        }
        manager::ImageStatus::Failed { timestamp } => Err(
            error::RequestImageError::DownloadFailed(url.to_string(), timestamp),
        ),
    }
}

#[derive(serde::Deserialize)]
struct MapUrlsOptions {
    style: Option<manager::UrlStyle>,
}

async fn map_urls(
    State(manager): State<Arc<Manager>>,
    Query(options): Query<MapUrlsOptions>,
    Json(urls): Json<Vec<String>>,
) -> Result<Json<Vec<Option<String>>>, error::MapUrlsError> {
    urls.into_iter()
        .map(|url| match manager.lookup_status(&url)? {
            manager::ImageStatus::Downloaded { entry } => Ok(Some(manager.static_url(
                entry.digest,
                entry.image_type.into(),
                options.style.unwrap_or_default(),
            ))),
            manager::ImageStatus::Downloading => Ok(Some(manager.request_url(
                &URL_SAFE_NO_PAD.encode(&url),
                options.style.unwrap_or_default(),
            ))),
            manager::ImageStatus::Failed { timestamp: _ } => Ok(None),
        })
        .collect::<Result<Vec<_>, error::MapUrlsError>>()
        .map(Json)
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Store initialization error")]
    StoreInitialization(#[from] image_cache::store::InitializationError),
    #[error("Index error")]
    IndexI(#[from] image_cache_index::db::Error),
}

#[derive(Debug, Parser)]
#[clap(name = "image-cache-service", version, author)]
struct Opts {
    #[command(flatten)]
    verbosity: clap_verbosity_flag::Verbosity,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    Serve {
        #[clap(long, default_value = "/")]
        base: String,
        #[clap(long, default_value = "0.0.0.0:3000")]
        server: String,
        #[clap(long)]
        store: PathBuf,
        #[clap(long)]
        prefix: PrefixPartLengths,
        #[clap(long)]
        index: PathBuf,
        #[clap(long, default_value = "8192")]
        buffer: usize,
        /// Time to wait between image requests in milliseconds
        #[clap(long, default_value = "500")]
        delay: u64,
    },
}
