mod tables;

use std::{collections::HashMap, error::Error, fs::File};

use clap::{Parser, Subcommand, ValueEnum};
use elasticsearch::{
    http::transport::Transport,
    indices::{IndicesCreateParts, IndicesDeleteParts, IndicesGetParts},
    params::Refresh,
    BulkOperation, BulkParts, Elasticsearch, SearchParts,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tables::Table;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct CommandLine {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    #[command(about = "Show information about the Elasticsearch service")]
    Info {},

    #[command(name = "ls")]
    #[command(about = "List available indexes")]
    GetIndexList {
        #[arg(short = 'a', long = "all")]
        #[arg(help = "Include indexes starting with '.'")]
        all: bool,
    },

    #[command(name = "mk")]
    #[command(about = "Create index")]
    CreateIndex {
        #[arg(help = "Name of the index to create")]
        index: String,
        #[arg(short = 'm', long = "mapping")]
        #[arg(help = "Field mapping")]
        mappings: Vec<String>,
    },

    #[command(name = "rm")]
    #[command(about = "Delete index")]
    DeleteIndex {
        #[arg(help = "Name of the index to delete")]
        index: String,
    },

    #[command(about = "Load data into an index")]
    Load {
        #[arg(help = "Name of the index to load into")]
        index: String,
        #[arg(short = 'c', long = "from-csv")]
        #[arg(help = "Filename of CSV file to load from")]
        csv_filenames: Vec<String>,
    },

    #[command(about = "Perform a search on an index")]
    Search {
        #[arg(help = "Name of the index to search")]
        index: String,
        #[arg(short = 'f', long = "format")]
        #[arg(help = "Output format for search results")]
        #[arg(default_value_t = SearchResultFormat::Raw, value_enum)]
        format: SearchResultFormat,
        #[arg(short = 's', long = "size")]
        #[arg(help = "Number of search hits to return")]
        size: Option<u16>,
    },
}

struct Es<'a> {
    elasticsearch: &'a Elasticsearch,
}

#[derive(Deserialize)]
struct EsInfo {
    name: String,
    cluster_name: String,
    version: EsInfoVersion,
    tagline: String,
}

#[derive(Deserialize)]
struct EsInfoVersion {
    number: String,
}

#[derive(Deserialize)]
struct EsCreated {
    acknowledged: bool,
    index: String,
}

#[derive(Deserialize)]
struct EsDeleted {
    acknowledged: bool,
}

#[derive(Deserialize)]
struct EsBulkSummary {
    items: Vec<HashMap<String, EsBulkSummaryAction>>,
}

#[derive(Deserialize)]
struct EsBulkSummaryAction {
    _index: String,
    _id: String,
    _version: i32,
    result: String,
    _seq_no: i32,
}

#[derive(Deserialize)]
struct EsSearchResult {
    hits: EsSearchResultHits,
}

#[derive(Deserialize)]
struct EsSearchResultHits {
    hits: Vec<EsSearchResultHitsHit>,
}

#[derive(Deserialize, Debug)]
struct EsSearchResultHitsHit {
    _index: String,
    _id: String,
    _score: f64,
    _source: HashMap<String, Value>,
}

impl Es<'_> {
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

    async fn create_index(
        &self,
        index: &str,
        mappings: &Vec<String>,
    ) -> Result<EsCreated, Box<dyn Error>> {
        let mut body = json!({
            "mappings": {
                "properties": {
                }
            }
        });
        for mapping in mappings.iter() {
            let bits: Vec<&str> = mapping.split(":").collect();
            body["mappings"]["properties"][bits[0]] = json!({"type": bits[1]});
        }
        let response = self
            .elasticsearch
            .indices()
            .create(IndicesCreateParts::Index(index))
            .body(body)
            .send()
            .await?;
        Ok(response.json::<EsCreated>().await?)
    }

    async fn delete_index(&self, index: &str) -> Result<EsDeleted, Box<dyn Error>> {
        let response = self
            .elasticsearch
            .indices()
            .delete(IndicesDeleteParts::Index(&[index]))
            .send()
            .await?;
        Ok(response.json::<EsDeleted>().await?)
    }

    async fn load(
        &self,
        index: &str,
        csv_filenames: &Vec<String>,
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

    async fn search(
        &self,
        index: &str,
        size: &Option<u16>,
    ) -> Result<EsSearchResult, Box<dyn Error>> {
        let mut body = json!({
            "query": {
                "match_all": {}
            }
        });
        match size {
            Some(x) => {
                body["size"] = json!(x);
            }
            _ => {}
        }
        let response = self
            .elasticsearch
            .search(SearchParts::Index(&[index]))
            .body(body)
            .send()
            .await?;
        Ok(response.json::<EsSearchResult>().await?)
    }
}

#[derive(ValueEnum, Copy, Clone, Debug, PartialEq, Eq)]
enum SearchResultFormat {
    Raw,
    Table,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let uri = "http://elastic:nmd6NZXM@localhost:9200";
    let args = CommandLine::parse();
    let transport = Transport::single_node(uri)?;
    let es = Es {
        elasticsearch: &Elasticsearch::new(transport),
    };
    match &args.command {
        Commands::Info {} => {
            print_info(&es.info().await?);
        }
        Commands::GetIndexList { all } => {
            print_index_list(&es.get_index_list(&["*"]).await?, all);
        }
        Commands::CreateIndex { index, mappings } => {
            let created = &es.create_index(index, mappings).await?;
            println!(
                "Created index {} ({}acknowledged)",
                created.index,
                if created.acknowledged { "" } else { "not " }
            );
        }
        Commands::DeleteIndex { index } => {
            let deleted = &es.delete_index(index).await?;
            println!(
                "Deleted index ({}acknowledged)",
                if deleted.acknowledged { "" } else { "not " }
            );
        }
        Commands::Load {
            index,
            csv_filenames,
        } => {
            let summary = &es.load(index, csv_filenames).await?;
            print_bulk_summary(summary);
        }
        Commands::Search {
            index,
            format,
            size,
        } => {
            let result = &es.search(index, size).await?;
            print_search_result(result, format);
        }
    }
    Ok(())
}

fn print_info(info: &EsInfo) {
    println!("Name: {}", info.name);
    println!("Cluster Name: {}", info.cluster_name);
    println!("Version: {}", info.version.number);
    println!("Tagline: {}", info.tagline);
}

fn print_index_list(index_list: &HashMap<String, Value>, all: &bool) {
    for (key, _value) in index_list.into_iter() {
        if *all || !key.starts_with(".") {
            println!("{} {}", key, _value);
        }
    }
}

fn print_bulk_summary(summary: &EsBulkSummary) {
    let mut results: HashMap<String, usize> = HashMap::new();
    for item in summary.items.iter() {
        for (_key, value) in item.into_iter() {
            *results.entry(value.result.to_string()).or_insert(0) += 1;
        }
    }
    for (actioned, count) in results.into_iter() {
        println!("Successfully {} {} documents", actioned, count);
    }
}

fn print_search_result(result: &EsSearchResult, format: &SearchResultFormat) {
    match format {
        SearchResultFormat::Raw => {
            for record in result.hits.hits.iter() {
                println!("{:?}", record);
            }
        }
        SearchResultFormat::Table => {
            let mut table = Table::new();
            for hit in result.hits.hits.iter() {
                table.push_row(&hit._source);
            }
            table.print();
        }
    }
}
