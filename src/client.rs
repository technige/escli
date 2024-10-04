use std::{
    collections::HashMap,
    env,
    fs::{read_to_string, File},
    path::Path,
};

use elasticsearch::{
    auth::Credentials,
    cat::CatIndicesParts,
    http::{
        transport::{SingleNodeConnectionPool, TransportBuilder},
        StatusCode, Url,
    },
    indices::{IndicesCreateParts, IndicesDeleteParts},
    params::{ExpandWildcards, Refresh},
    BulkOperation, BulkParts, Elasticsearch, SearchParts,
};
use serde::Deserialize;
use serde_json::{json, Value};

pub struct SimpleClient {
    url: Url,
    elasticsearch: Elasticsearch,
}

impl SimpleClient {
    /// Creates a new client with the given URL and credentials.
    ///
    pub fn new(url: Url, auth: Credentials) -> Self {
        Self {
            url: url.clone(),
            elasticsearch: Elasticsearch::new(
                TransportBuilder::new(SingleNodeConnectionPool::new(url))
                    .auth(auth)
                    .build()
                    .expect("Failed to create transport"),
            ),
        }
    }

    /// Creates a new client by first checking environment variables, then
    /// sniffing for a _start-local_ `.env` file, if these are not found.
    /// Overall, the sequence of checks is as follows:
    ///
    /// 1. Check for `ESCLI_URL` and `ESCLI_API_KEY` env vars
    /// 2. Check for `ESCLI_URL` and `ESCLI_USER`/`ESCLI_PASSWORD` env vars
    /// 3. Check for `.env` file in current directory
    /// 4. Check for `.env` file in `elastic-start-local` subdirectory
    /// 5. Give up and fail
    ///
    pub fn default() -> Result<Self, Error> {
        match Self::from_env_vars() {
            Ok(client) => Ok(client),
            Err(_) => {
                match Self::for_start_local(Path::new(".")) {
                    Ok(client) => Ok(client),
                    Err(_) => match Self::for_start_local(Path::new("elastic-start-local")) {
                        Ok(client) => Ok(client),
                        Err(_) => {
                            Err(Error::new(
                                ErrorType::ConfigurationError,
                                "failed to initialise client from either environment variables or start-local .env file".to_string()
                            ))
                        }
                    },
                }
            }
        }
    }

    /// Creates a new client by reading configuration values from environment
    /// variables.
    ///
    /// - `ESCLI_URL` - URL of Elasticsearch service (e.g. `http://localhost:9200`)
    /// - `ESCLI_USER` - user name for authentication (default `elastic`)
    /// - `ESCLI_PASSWORD` - password for authentication
    /// - `ESCLI_API_KEY` - API key for authentication
    ///
    /// A URL is required, but it is not necessary to provide values for all
    /// authentication variables. Either `ESCLI_USER`/`ESCLI_PASSWORD` or
    /// `ESCLI_API_KEY` may be supplied.
    ///
    pub fn from_env_vars() -> Result<Self, Error> {
        match env::var("ESCLI_URL") {
            Ok(url) => match Url::parse(url.as_str()) {
                Ok(url) => {
                    let auth;
                    match env::var("ESCLI_API_KEY") {
                        Ok(api_key) => {
                            auth = Credentials::EncodedApiKey(api_key);
                        }
                        Err(_) => match env::var("ESCLI_PASSWORD") {
                            Ok(password) => {
                                auth = Credentials::Basic(
                                    env::var("ESCLI_USER").unwrap_or(String::from("elastic")),
                                    password,
                                );
                            }
                            Err(e) => {
                                return Err(Error::new(
                                    ErrorType::ConfigurationError,
                                    format!("failed to load Elasticsearch credentials from either ESCLI_API_KEY or ESCLI_USER/ESCLI_PASSWORD ({e})")
                                ));
                            }
                        },
                    }
                    Ok(Self::new(url, auth))
                }
                Err(e) => Err(Error::new(
                    ErrorType::ConfigurationError,
                    format!("failed to parse ESCLI_URL ({e})"),
                )),
            },
            Err(e) => Err(Error::new(
                ErrorType::ConfigurationError,
                format!("failed to load Elasticsearch URL from ESCLI_URL ({e})"),
            )),
        }
    }

    pub fn for_start_local(path: &Path) -> Result<Self, Error> {
        match read_to_string(path.join(".env")) {
            Ok(string) => {
                let mut env_vars: HashMap<&str, &str> = HashMap::new();
                for line in string.lines() {
                    if let Some((name, value)) = line.split_once('=') {
                        env_vars.insert(name, value);
                    }
                }
                let url_str = format!(
                    "http://localhost:{}",
                    match env_vars.get("ES_LOCAL_PORT") {
                        Some(port) => port,
                        None => "9200",
                    }
                );

                let url = match Url::parse(url_str.as_str()) {
                    Ok(parsed) => parsed,
                    Err(e) => {
                        return Err(Error::new(
                            ErrorType::ConfigurationError,
                            format!("failed to parse URL {url_str} ({e})"),
                        ));
                    }
                };
                let auth = match env_vars.get("ES_LOCAL_API_KEY") {
                    Some(api_key) => Credentials::EncodedApiKey(api_key.to_string()),
                    None => {
                        return Err(Error::new(
                            ErrorType::ConfigurationError,
                            "could not find ES_LOCAL_API_KEY in start-local .env file".to_string(),
                        ));
                    }
                };
                Ok(Self::new(url, auth))
            }
            Err(e) => Err(Error::new(
                ErrorType::ConfigurationError,
                format!("failed to load Elasticsearch details from start-local .env file ({e})"),
            )),
        }
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    pub async fn ping(&self) -> Result<StatusCode, Error> {
        match self.elasticsearch.ping().send().await {
            Ok(response) => Ok(response.status_code()),
            Err(e) => Err(Error::from_client_error(&e)),
        }
    }

    pub async fn info(&self) -> Result<RawInfo, Error> {
        match self.elasticsearch.info().send().await {
            Ok(response) => match response.json::<RawInfo>().await {
                Ok(info) => Ok(info),
                Err(e) => Err(Error::from_client_error(&e)),
            },
            Err(e) => Err(Error::from_client_error(&e)),
        }
    }

    pub async fn get_index_list(
        &self,
        patterns: &[&str],
        all: bool,
        open: bool,
        closed: bool,
    ) -> Result<Vec<IndexDetail>, Error> {
        match self
            .elasticsearch
            .cat()
            .indices(CatIndicesParts::Index(patterns))
            .format("json")
            .bytes(elasticsearch::params::Bytes::B)
            .expand_wildcards(if all && open && closed {
                &[
                    ExpandWildcards::Open,
                    ExpandWildcards::Closed,
                    ExpandWildcards::Hidden,
                ]
            } else if all && open {
                &[ExpandWildcards::Open, ExpandWildcards::Hidden]
            } else if all && closed {
                &[ExpandWildcards::Closed, ExpandWildcards::Hidden]
            } else if open && closed {
                &[ExpandWildcards::Open, ExpandWildcards::Closed]
            } else if all {
                &[ExpandWildcards::All]
            } else if open {
                &[ExpandWildcards::Open]
            } else if closed {
                &[ExpandWildcards::Closed]
            } else {
                &[ExpandWildcards::Open]
            })
            .send()
            .await
        {
            Ok(response) => match response.status_code().as_u16() {
                200..=299 => Ok(match response.json::<Vec<HashMap<String, Value>>>().await {
                    Ok(raw) => raw
                        .iter()
                        .map(|entry| IndexDetail {
                            health: entry["health"].as_str().unwrap_or("unknown").to_string(),
                            status: entry["status"].as_str().unwrap_or("unknown").to_string(),
                            name: entry["index"].as_str().unwrap_or("unknown").to_string(),
                            uuid: entry["uuid"].as_str().unwrap_or("unknown").to_string(),
                            docs_count: match entry["docs.count"].as_str() {
                                Some(string_value) => match string_value.parse::<u64>() {
                                    Ok(value) => Some(value),
                                    Err(_) => None,
                                },
                                None => None,
                            },
                            docs_deleted: match entry["docs.deleted"].as_str() {
                                Some(string_value) => match string_value.parse::<u64>() {
                                    Ok(value) => Some(value),
                                    Err(_) => None,
                                },
                                None => None,
                            },
                            store_size: match entry["store.size"].as_str() {
                                Some(string_value) => match string_value.parse::<u64>() {
                                    Ok(value) => Some(value),
                                    Err(_) => None,
                                },
                                None => None,
                            },
                            dataset_size: match entry["dataset.size"].as_str() {
                                Some(string_value) => match string_value.parse::<u64>() {
                                    Ok(value) => Some(value),
                                    Err(_) => None,
                                },
                                None => None,
                            },
                        })
                        .collect(),
                    Err(e) => {
                        // failed to decode response body
                        return Err(Error::from_client_error(&e));
                    }
                }),
                _ => Err(Error::from_server_error(
                    // did not receive 2xx status code
                    &match response.json::<RawError>().await {
                        Ok(raw) => raw,
                        Err(e) => {
                            // failed to decode error response body
                            return Err(Error::from_client_error(&e));
                        }
                    },
                )),
            },
            Err(e) => {
                // failed to carry out request-response
                Err(Error::from_client_error(&e))
            }
        }
    }

    pub async fn create_index(
        &self,
        index: &str,
        mappings: &[String],
    ) -> Result<RawCreated, Error> {
        let mut body = json!({
            "mappings": {
                "properties": {
                }
            }
        });
        for mapping in mappings.iter() {
            let bits: Vec<&str> = mapping.split(':').collect();
            body["mappings"]["properties"][bits[0]] = json!({"type": bits[1]});
        }
        match self
            .elasticsearch
            .indices()
            .create(IndicesCreateParts::Index(index))
            .body(body)
            .send()
            .await
        {
            Ok(response) => match response.status_code().as_u16() {
                200..=299 => Ok(match response.json::<RawCreated>().await {
                    Ok(raw) => raw,
                    Err(e) => return Err(Error::from_client_error(&e)),
                }),
                _ => Err(Error::from_server_error(
                    &match response.json::<RawError>().await {
                        Ok(raw) => raw,
                        Err(e) => return Err(Error::from_client_error(&e)),
                    },
                )),
            },
            Err(e) => Err(Error::from_client_error(&e)),
        }
    }

    pub async fn delete_index(
        &self,
        index: &str,
    ) -> Result<RawDeleted, Box<dyn std::error::Error>> {
        match self
            .elasticsearch
            .indices()
            .delete(IndicesDeleteParts::Index(&[index]))
            .send()
            .await
        {
            Ok(response) => match response.status_code().as_u16() {
                200..=299 => Ok(response.json::<RawDeleted>().await?),
                _ => Err(Box::from(Error::from_server_error(
                    &response.json::<RawError>().await?,
                ))),
            },
            Err(error) => Err(Box::from(error)),
        }
    }

    pub async fn load(
        &self,
        index: &str,
        csv_filenames: &[String],
    ) -> Result<RawBulkSummary, Box<dyn std::error::Error>> {
        type Document = HashMap<String, Value>;
        let mut documents: Vec<Document> = Vec::new();
        for filename in csv_filenames.iter() {
            let file = File::open(filename)?;
            let mut reader = csv::Reader::from_reader(file);
            for result in reader.deserialize() {
                let document: Document = result?;
                documents.push(document);
            }
        }
        let mut body: Vec<BulkOperation<_>> = vec![];
        for document in documents.iter() {
            body.push(BulkOperation::index(json!(document)).into());
        }
        let response = self
            .elasticsearch
            .bulk(BulkParts::Index(index))
            .body(body)
            .refresh(Refresh::WaitFor)
            .send()
            .await?;
        Ok(response.json::<RawBulkSummary>().await?)
    }

    pub async fn search(
        &self,
        index: &str,
        query: &Option<String>,
        order_by: &Option<String>,
        limit: &Option<u16>,
    ) -> Result<RawSearchResult, Error> {
        let target = &[index];
        let mut request = self.elasticsearch.search(SearchParts::Index(target));
        let mut order_by_pairs = Vec::new();
        let mut body = json!({});
        match query {
            Some(x) => request = request.q(x),
            _ => body["query"] = json!({"match_all": {}}),
        }
        if let Some(x) = order_by {
            order_by_pairs.push(x.as_str());
            request = request.sort(order_by_pairs.as_slice())
        }
        if let Some(x) = limit {
            body["size"] = json!(x);
        }
        match request.body(body).send().await {
            Ok(response) => match response.status_code().as_u16() {
                200..=299 => Ok(match response.json::<RawSearchResult>().await {
                    Ok(data) => data,
                    Err(e) => return Err(Error::from_client_error(&e)), // failed to decode search response body
                }),
                _ => Err(Error::from_server_error(
                    &match response.json::<RawError>().await {
                        Ok(data) => data,
                        Err(e) => return Err(Error::from_client_error(&e)), // failed to decode error response body
                    },
                )),
            },
            Err(e) => Err(Error::from_client_error(&e)), // failed to send
        }
    }
}

#[derive(Debug)]
pub enum ErrorType {
    ConfigurationError,
    ClientError,
    ServerError(u16),
}

#[derive(Debug)]
pub struct Error {
    subtype: ErrorType,
    description: String,
}

impl Error {
    pub fn new(subtype: ErrorType, description: String) -> Self {
        Error {
            subtype,
            description,
        }
    }

    pub fn from_client_error(error: &elasticsearch::Error) -> Self {
        Error {
            subtype: ErrorType::ClientError,
            description: error.to_string(),
        }
    }

    pub fn from_server_error(raw_error: &RawError) -> Self {
        let detail: &RawErrorDetail = if raw_error
            .error
            .root_cause
            .as_ref()
            .is_some_and(|x| !x.is_empty())
        {
            &raw_error.error.root_cause.as_ref().unwrap()[0]
        } else {
            &raw_error.error
        };
        Error {
            subtype: ErrorType::ServerError(raw_error.status),
            description: detail
                .reason
                .as_ref()
                .unwrap_or(&raw_error.error.type_code)
                .to_string(),
        }
    }
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.subtype, self.description)
    }
}

#[derive(Deserialize, Debug)]
pub struct RawError {
    pub error: RawErrorDetail,
    pub status: u16,
}

#[derive(Deserialize, Debug)]
pub struct RawErrorDetail {
    #[serde(rename = "type")]
    pub type_code: String,
    pub reason: Option<String>,
    pub root_cause: Option<Vec<RawErrorDetail>>,
}

impl std::error::Error for RawError {}

impl std::fmt::Display for RawError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.error.reason {
            Some(ref text) => write!(f, "Error: {}", text),
            _ => write!(f, "Error: {}", self.error.type_code),
        }
    }
}

#[derive(Deserialize)]
pub struct RawInfo {
    pub name: String,
    pub cluster_name: String,
    pub cluster_uuid: String,
    pub version: RawInfoVersion,
    pub tagline: String,
}

#[derive(Deserialize)]
pub struct RawInfoVersion {
    pub number: String,
    pub build_flavor: String,
    pub build_type: String,
    pub build_hash: String,
    pub build_date: String,
    pub build_snapshot: bool,
    pub lucene_version: String,
    pub minimum_wire_compatibility_version: String,
    pub minimum_index_compatibility_version: String,
}

#[derive(Deserialize)]
pub struct RawCreated {
    pub acknowledged: bool,
    pub index: String,
}

#[derive(Deserialize)]
pub struct RawDeleted {
    pub acknowledged: bool,
}

#[derive(Deserialize)]
pub struct RawBulkSummary {
    pub items: Vec<HashMap<String, RawBulkSummaryAction>>,
}

#[derive(Deserialize)]
pub struct RawBulkSummaryAction {
    pub _index: String,
    pub _id: String,
    pub _version: i32,
    pub result: String,
    pub _seq_no: i32,
}

#[derive(Deserialize)]
pub struct RawSearchResult {
    pub hits: RawSearchResultHits,
}

#[derive(Deserialize)]
pub struct RawSearchResultHits {
    pub hits: Vec<RawSearchResultHitsHit>,
}

#[derive(Deserialize, Debug)]
pub struct RawSearchResultHitsHit {
    pub _index: String,
    pub _id: String,
    pub _score: Option<f64>,
    pub _source: HashMap<String, Value>,
}

pub struct IndexDetail {
    pub health: String,
    pub status: String,
    pub name: String,
    pub uuid: String,
    pub docs_count: Option<u64>,
    pub docs_deleted: Option<u64>,
    pub store_size: Option<u64>,
    pub dataset_size: Option<u64>,
}
