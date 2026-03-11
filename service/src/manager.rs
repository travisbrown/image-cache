use chrono::{DateTime, Utc};
use image_cache::{client::Client, image_type::ImageType, store::Store};
use image_cache_index::{Entry, db::Database};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
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

/// A running per-domain download worker: owns its own HTTP client and applies
/// the configured inter-request delay only when back-to-back requests arrive.
struct DomainWorker {
    sender: Sender<(String, oneshot::Sender<ClientResult>)>,
    handle: JoinHandle<()>,
}

impl DomainWorker {
    /// Spawns a new worker task and returns a handle to its queue.
    fn spawn(client: Client, delay: Duration, buffer_size: usize) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel(buffer_size);
        let handle = tokio::task::spawn(Self::run(client, delay, receiver));
        Self { sender, handle }
    }

    /// Processes download requests sequentially, rate-limiting only when
    /// another request is already queued (i.e. back-to-back within this domain).
    /// The task exits cleanly when all senders are dropped.
    async fn run(
        client: Client,
        delay: Duration,
        mut receiver: Receiver<(String, oneshot::Sender<ClientResult>)>,
    ) {
        // Holds a request peeked non-blocking after the previous download so
        // we can decide whether to sleep before processing it.
        let mut prefetched: Option<(String, oneshot::Sender<ClientResult>)> = None;

        loop {
            // Use the already-dequeued item if present; otherwise block for one.
            let (url, reply) = match prefetched.take() {
                Some(item) => item,
                None => match receiver.recv().await {
                    Some(item) => item,
                    None => break, // all senders dropped → clean shutdown
                },
            };

            log::info!("Downloading image: {url}");
            let result = client.download(&url).await;

            if reply.send(result).is_err() {
                log::warn!("Image already downloaded (may need to re-index image store): {url}");
            }

            // Non-blocking peek: sleep only when another request is already
            // queued. If the domain queue is idle the next recv() blocks with
            // no artificial delay.
            match receiver.try_recv() {
                Ok(item) => {
                    prefetched = Some(item);
                    log::info!("Waiting until next download: {delay:?}");
                    tokio::time::sleep(delay).await;
                }
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => break,
            }
        }
    }
}

pub struct Manager {
    url_config: UrlConfig,
    pub index: Database,
    store: Store,
    /// Template client cloned into each new per-domain worker.
    client: Client,
    /// Inter-request delay applied per domain queue.
    delay: Duration,
    /// Channel buffer size used when spawning new domain workers.
    request_buffer_size: usize,
    /// Per-domain download workers, keyed by host (e.g. `"example.com"`).
    /// Workers are spawned lazily on the first request for each host.
    domain_workers: Mutex<HashMap<String, DomainWorker>>,
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
        let client = Client::new(store.clone());
        let index = Database::open(index)?;

        Ok(Self {
            url_config,
            store,
            index,
            client,
            delay,
            request_buffer_size,
            domain_workers: Mutex::new(HashMap::new()),
        })
    }

    pub async fn close(&self) -> Result<(), super::error::ShutdownError> {
        let workers = std::mem::take(&mut *self.domain_workers.lock().await);

        for (_, worker) in workers {
            // Dropping the sender closes the channel, which causes the worker
            // task to exit its recv() loop cleanly.
            drop(worker.sender);
            worker.handle.await?;
        }

        Ok(())
    }

    // The MutexGuard `workers` is dropped at the end of the inner block, before
    // the potentially-slow channel send. Clippy still considers the drop scope
    // improvable even though it is already at the earliest possible point.
    #[allow(clippy::significant_drop_tightening)]
    pub async fn request(
        &self,
        image_url: &str,
    ) -> Result<ClientResult, super::error::ChannelError> {
        let domain = Self::domain_of(image_url);
        let (tx, rx) = oneshot::channel();

        let sender = {
            let mut workers = self.domain_workers.lock().await;
            workers
                .entry(domain.clone())
                .or_insert_with(|| {
                    log::info!("Creating worker for {domain}");
                    DomainWorker::spawn(
                        self.client.clone(),
                        self.delay,
                        self.request_buffer_size,
                    )
                })
                .sender
                .clone()
        };

        sender.send((image_url.to_string(), tx)).await?;
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
}
