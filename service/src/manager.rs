use chrono::{DateTime, Utc};
use image_cache::{client::Client, image_type::ImageType, store::Store};
use image_cache_index::{Entry, db::Database};
use std::collections::HashMap;
use std::sync::Arc;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    sync::{
        Mutex,
        mpsc::{Receiver, Sender, error::TryRecvError},
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

/// A running per-domain download worker: the send end of its queue and its task handle.
struct DomainWorker {
    sender: Sender<Option<(String, oneshot::Sender<ClientResult>)>>,
    handle: JoinHandle<()>,
}

pub struct Manager {
    url_config: UrlConfig,
    pub index: Database,
    store: Store,
    /// Shared HTTP client, cloned into each per-domain worker task.
    client: Arc<Client>,
    /// Inter-request delay applied per domain (not globally).
    delay: Duration,
    /// Channel buffer size reused when spawning new domain workers.
    request_buffer_size: usize,
    /// Per-domain download queues, keyed by host (e.g. `"example.com"`).
    /// Workers are spawned lazily on the first request for each host.
    domain_workers: Arc<Mutex<HashMap<String, DomainWorker>>>,
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

        Ok(Self {
            url_config,
            store,
            index,
            client,
            delay,
            request_buffer_size,
            domain_workers: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub async fn close(&self) -> Result<(), super::error::ShutdownError> {
        // Drain the map so all workers are owned here.
        let workers = std::mem::take(&mut *self.domain_workers.lock().await);

        for (_, worker) in workers {
            worker.sender.send(None).await?;
            worker.handle.await?;
        }

        Ok(())
    }

    // The MutexGuard `workers` is already dropped at the earliest possible point
    // (immediately after `worker.sender.clone()`), so clippy's suggestion to
    // tighten the drop scope further is a false positive here.
    #[allow(clippy::significant_drop_tightening)]
    pub async fn request(
        &self,
        image_url: &str,
    ) -> Result<ClientResult, super::error::ChannelError> {
        let domain = Self::domain_of(image_url);
        let (tx, rx) = oneshot::channel();

        // Hold the lock only long enough to look up or create the per-domain sender,
        // then release it before the potentially-blocking channel send.
        let sender = {
            let mut workers = self.domain_workers.lock().await;
            let worker = workers.entry(domain.clone()).or_insert_with(|| {
                log::info!("Creating worker for {domain}");

                let (sender, receiver) = tokio::sync::mpsc::channel(self.request_buffer_size);
                let handle = Self::handle_requests(Arc::clone(&self.client), self.delay, receiver);
                DomainWorker { sender, handle }
            });
            worker.sender.clone()
        };

        sender.send(Some((image_url.to_string(), tx))).await?;
        rx.await.map_err(super::error::ChannelError::from)
    }

    /// Extracts the host component of a URL (e.g. `"example.com"` from
    /// `"https://example.com/img.png"`). Falls back to an empty string for
    /// malformed URLs so they all share a single fallback queue.
    fn domain_of(url: &str) -> String {
        url::Url::parse(url)
            .ok()
            .and_then(|u| u.host_str().map(str::to_string))
            .unwrap_or_default()
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
            // Holds a request that was dequeued non-blocking at the end of the
            // previous iteration (used to decide whether to sleep).
            let mut prefetched: Option<(String, oneshot::Sender<ClientResult>)> = None;

            loop {
                // Use the prefetched item if available; otherwise block until one arrives.
                let (url, sender) = match prefetched.take() {
                    Some(item) => item,
                    // None payload = shutdown signal; channel closed = same.
                    None => if let Some(Some(item)) = receiver.recv().await { item } else {
                        receiver.close();
                        break;
                    },
                };

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

                // Non-blocking peek: only sleep when the next request for this
                // domain is already queued (back-to-back). When the queue is idle
                // the next recv() will block immediately with no added delay.
                match receiver.try_recv() {
                    Ok(Some(item)) => {
                        prefetched = Some(item);
                        log::info!("Waiting until next download: {delay:?}");
                        tokio::time::sleep(delay).await;
                    }
                    Ok(None) | Err(TryRecvError::Disconnected) => {
                        receiver.close();
                        break;
                    }
                    // Queue is empty; loop back to recv() with no sleep.
                    Err(TryRecvError::Empty) => {}
                }
            }
        })
    }
}
