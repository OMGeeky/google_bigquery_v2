use std::error::Error;
use std::fmt::Debug;

use google_bigquery2::hyper::client::HttpConnector;
use google_bigquery2::hyper_rustls::HttpsConnector;
use google_bigquery2::Bigquery;
use google_bigquery2::{hyper, hyper_rustls, oauth2};

#[derive(Clone)]
pub struct BigqueryClient {
    client: Bigquery<HttpsConnector<HttpConnector>>,
    project_id: String,
    dataset_id: String,
}

impl Default for BigqueryClient {
    fn default() -> Self {
        BigqueryClient::empty()
    }
}

impl BigqueryClient {
    pub fn empty() -> BigqueryClient {
        todo!()
    }
}

//TODO: check if this unsafe impl is needed
unsafe impl Send for BigqueryClient {}

//TODO: check if this unsafe impl is needed
unsafe impl Sync for BigqueryClient {}

impl BigqueryClient {
    pub async fn new<S: Into<String>>(
        project_id: S,
        dataset_id: S,
        service_account_path: Option<S>,
    ) -> Result<BigqueryClient, Box<dyn Error>> {
        let client = get_internal_client(service_account_path).await?;
        Ok(BigqueryClient {
            client,
            project_id: project_id.into(),
            dataset_id: dataset_id.into(),
        })
    }

    pub fn get_client(&self) -> &Bigquery<HttpsConnector<HttpConnector>> {
        &self.client
    }
    pub fn get_project_id(&self) -> &str {
        &self.project_id
    }
    pub fn get_dataset_id(&self) -> &str {
        &self.dataset_id
    }
}

impl Debug for BigqueryClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BigqueryClient")
            .field("project_id", &self.project_id)
            .field("dataset_id", &self.dataset_id)
            .finish()
    }
}

async fn get_internal_client<S: Into<String>>(
    service_account_path: Option<S>,
) -> Result<Bigquery<HttpsConnector<HttpConnector>>, Box<dyn Error>> {
    let hyper_client = hyper::Client::builder().build(
        hyper_rustls::HttpsConnectorBuilder::new()
            .with_native_roots()
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build(),
    );
    let service_account_path = match service_account_path {
        None => "auth/service_account2.json".to_string(),
        Some(s) => s.into(),
    };
    let secret = oauth2::read_service_account_key(&service_account_path)
        .await
        .expect(
            format!(
                "Failed to read service account key from file. {}",
                service_account_path
            )
            .as_str(),
        );
    let auth = oauth2::ServiceAccountAuthenticator::builder(secret)
        .build()
        .await
        .expect("Failed to authenticate with service account key.");
    let client: Bigquery<HttpsConnector<HttpConnector>> = Bigquery::new(hyper_client, auth);

    Ok(client)
}
