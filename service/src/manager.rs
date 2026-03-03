use chrono::{DateTime, Utc};
use futures::future::TryFutureExt;
use image_cache::{client::Client, image_type::ImageType, store::Store};
use image_cache_index::{Entry, db::Database};
use std::sync::Arc;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    sync::{
        Mutex,
        mpsc::{Receiver, Sender},
        oneshot,
    },
    task::JoinHandle,
};

pub type ClientResult = Result<
    Result<(bytes::Bytes, image_cache::store::Action), http::StatusCode>,
    image_cache::client::Error,
>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UrlConfig {
    pub secure: bool,
    pub server: String,
    pub base_path: String,
}

impl UrlConfig {
    #[must_use]
    pub const fn new(secure: bool, server: String, base_path: String) -> Self {
        Self {
            secure,
            server,
            base_path,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UrlStyle {
    #[default]
    Full,
    Absolute,
    Relative,
}

pub struct Manager {
    url_config: UrlConfig,
    pub index: Database,
    store: Store,
    request_sender: Sender<Option<(String, oneshot::Sender<ClientResult>)>>,
    request_receiver_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

pub enum ImageStatus {
    Downloaded { entry: Entry },
    Downloading,
    Failed { timestamp: DateTime<Utc> },
}

impl Manager {
    pub fn new<I: AsRef<Path>>(
        url_config: UrlConfig,
        store: Store,
        index: I,
        request_buffer_size: usize,
        delay: Duration,
    ) -> Result<Self, image_cache_index::db::Error> {
        let client = Arc::new(Client::new(store.clone()));
        let index = Database::open(index)?;

        let (request_sender, request_receiver) = tokio::sync::mpsc::channel(request_buffer_size);

        Ok(Self {
            url_config,
            store,
            index,
            request_sender,
            request_receiver_handle: Arc::new(Mutex::new(Some(Self::handle_requests(
                client,
                delay,
                request_receiver,
            )))),
        })
    }

    pub async fn close(&self) -> Result<(), super::error::ShutdownError> {
        self.request_sender.send(None).await?;
        let handle = self.request_receiver_handle.lock().await.take();
        if let Some(handle) = handle {
            handle.await?;
        }

        Ok(())
    }

    pub fn request(
        &self,
        image_url: &str,
    ) -> impl Future<Output = Result<ClientResult, super::error::ChannelError>> {
        let (sender, receiver) = oneshot::channel();

        self.request_sender
            .send(Some((image_url.to_string(), sender)))
            .map_err(super::error::ChannelError::from)
            .and_then(|()| receiver.map_err(super::error::ChannelError::from))
    }

    pub fn lookup_status(
        &self,
        image_url: &str,
    ) -> Result<ImageStatus, image_cache_index::db::Error> {
        let results = self.index.lookup(image_url)?;

        if results.is_empty() {
            Ok(ImageStatus::Downloading)
        } else {
            let entry = results.iter().find_map(|result| result.ok());

            entry.map_or_else(
                || {
                    // We should always find a value because of the empty check above.
                    let timestamp = results
                        .iter()
                        .find_map(|result| result.err())
                        .expect("Expected at least one error (should never happen)");

                    Ok(ImageStatus::Failed { timestamp })
                },
                |entry| Ok(ImageStatus::Downloaded { entry }),
            )
        }
    }

    pub fn path_for_digest(&self, digest: md5::Digest) -> Option<PathBuf> {
        let path = self.store.path(digest);

        if path.exists() && path.is_file() {
            Some(path)
        } else {
            None
        }
    }

    pub fn static_url(
        &self,
        digest: md5::Digest,
        image_type: ImageType,
        style: UrlStyle,
    ) -> String {
        let image_type_str = image_type.as_str();

        let mut prefix = String::new();

        if style == UrlStyle::Full {
            prefix.push_str(if self.url_config.secure {
                "https://"
            } else {
                "http://"
            });

            prefix.push_str(&self.url_config.server);
        }

        if style != UrlStyle::Relative {
            prefix.push_str(&self.url_config.base_path);
        }

        if image_type_str.is_empty() {
            format!("{prefix}static/{digest:x}")
        } else {
            format!("{prefix}static/{digest:x}.{image_type}")
        }
    }

    pub fn request_url(&self, encoded_url: &str, style: UrlStyle) -> String {
        let mut prefix = String::new();

        if style == UrlStyle::Full {
            prefix.push_str(if self.url_config.secure {
                "https://"
            } else {
                "http://"
            });

            prefix.push_str(&self.url_config.server);
        }

        if style != UrlStyle::Relative {
            prefix.push_str(&self.url_config.base_path);
        }

        format!("{prefix}request/{encoded_url}")
    }

    fn handle_requests(
        client: Arc<Client>,
        delay: Duration,
        mut receiver: Receiver<Option<(String, oneshot::Sender<ClientResult>)>>,
    ) -> JoinHandle<()> {
        tokio::task::spawn(async move {
            while let Some(request) = receiver.recv().await {
                if let Some((url, sender)) = request {
                    log::info!("Downloading image: {url}");
                    let result = client.download(&url).await;

                    match sender.send(result) {
                        Ok(()) => {}
                        Err(_result) => {
                            log::warn!(
                                "Image already downloaded (may need to re-index image store): {url})"
                            );
                        }
                    }

                    log::info!("Waiting until next download: {delay:?}");
                    tokio::time::sleep(delay).await;
                } else {
                    receiver.close();
                    break;
                }
            }
        })
    }
}
