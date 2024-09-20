use std::{collections::HashMap, error::Error, fs::File};

use elasticsearch::{
    auth::Credentials,
    http::{
        transport::{SingleNodeConnectionPool, TransportBuilder},
        StatusCode, Url,
    },
    indices::{IndicesCreateParts, IndicesDeleteParts, IndicesGetParts},
    params::Refresh,
    BulkOperation, BulkParts, Elasticsearch, SearchParts,
};
use serde::Deserialize;
use serde_json::{json, Value};

pub struct Es {
    url: Url,
    elasticsearch: Elasticsearch,
}

#[derive(Deserialize)]
pub struct EsInfo {
    pub name: String,
    pub cluster_name: String,
    pub cluster_uuid: String,
    pub version: EsInfoVersion,
    pub tagline: String,
}

#[derive(Deserialize)]
pub struct EsInfoVersion {
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
pub struct EsCreated {
    pub acknowledged: bool,
    pub index: String,
}

#[derive(Deserialize)]
pub struct EsDeleted {
    pub acknowledged: bool,
}

#[derive(Deserialize)]
pub struct EsBulkSummary {
    pub items: Vec<HashMap<String, EsBulkSummaryAction>>,
}

#[derive(Deserialize)]
pub struct EsBulkSummaryAction {
    pub _index: String,
    pub _id: String,
    pub _version: i32,
    pub result: String,
    pub _seq_no: i32,
}

#[derive(Deserialize)]
pub struct EsSearchResult {
    pub hits: EsSearchResultHits,
}

#[derive(Deserialize)]
pub struct EsSearchResultHits {
    pub hits: Vec<EsSearchResultHitsHit>,
}

#[derive(Deserialize, Debug)]
pub struct EsSearchResultHitsHit {
    pub _index: String,
    pub _id: String,
    pub _score: Option<f64>,
    pub _source: HashMap<String, Value>,
}

impl Es {
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

    pub fn url(&self) -> &Url {
        &self.url
    }

    pub async fn ping(&self) -> Result<StatusCode, Box<dyn Error>> {
        let response = self.elasticsearch.ping().send().await?;
        Ok(response.status_code())
    }

    pub async fn info(&self) -> Result<EsInfo, Box<dyn Error>> {
        let response = self.elasticsearch.info().send().await?;
        Ok(response.json::<EsInfo>().await?)
    }

    pub async fn get_index_list(
        &self,
        patterns: &[&str],
    ) -> Result<HashMap<String, Value>, Box<dyn Error>> {
        let response = self
            .elasticsearch
            .indices()
            .get(IndicesGetParts::Index(patterns))
            .send()
            .await?;
        Ok(response.json::<HashMap<String, Value>>().await?)
    }

    pub async fn create_index(
        &self,
        index: &str,
        mappings: &[String],
    ) -> Result<EsCreated, Box<dyn Error>> {
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
                200 => Ok(response.json::<EsCreated>().await?),
                _ => Ok(response.json::<EsCreated>().await?),
            },
            Err(error) => Err(Box::from(error)),
        }
    }

    pub async fn delete_index(&self, index: &str) -> Result<EsDeleted, Box<dyn Error>> {
        let response = self
            .elasticsearch
            .indices()
            .delete(IndicesDeleteParts::Index(&[index]))
            .send()
            .await?;
        Ok(response.json::<EsDeleted>().await?)
    }

    pub async fn load(
        &self,
        index: &str,
        csv_filenames: &[String],
    ) -> Result<EsBulkSummary, Box<dyn Error>> {
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
        Ok(response.json::<EsBulkSummary>().await?)
    }

    pub async fn search(
        &self,
        index: &str,
        query: &Option<String>,
        order_by: &Option<String>,
        limit: &Option<u16>,
    ) -> Result<EsSearchResult, Box<dyn Error>> {
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
        let response = request.body(body).send().await?;
        Ok(response.json::<EsSearchResult>().await?)
    }
}
