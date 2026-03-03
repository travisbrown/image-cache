use crate::store::{Action, Store};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("HTTP client error")]
    Http(#[from] reqwest::Error),
    #[error("Store error")]
    Store(#[from] prefix_file_tree::Error),
}

#[derive(Clone)]
pub struct Client {
    underlying: reqwest::Client,
    store: Store,
}

impl Client {
    #[must_use]
    pub fn new(store: Store) -> Self {
        Self {
            underlying: reqwest::Client::default(),
            store,
        }
    }

    pub async fn download(
        &self,
        url: &str,
    ) -> Result<Result<(bytes::Bytes, Action), http::StatusCode>, Error> {
        let response = self.underlying.get(url).send().await?;
        let status_code = response.status();

        if status_code == reqwest::StatusCode::OK {
            let bytes = response.bytes().await?;
            let action = self.store.save(&bytes)?;

            Ok(Ok((bytes, action)))
        } else {
            Ok(Err(status_code))
        }
    }
}
