use std::{collections::HashMap, fs::File};

use clap::{Parser, Subcommand};
use elasticsearch::{
    http::transport::Transport,
    indices::{IndicesCreateParts, IndicesDeleteParts, IndicesGetParts},
    params::Refresh,
    BulkOperation, BulkParts, Elasticsearch, SearchParts,
};
use serde::Deserialize;
use serde_json::{json, Value};

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
    ListIndexes {
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
        #[arg(short = 's', long = "size")]
        #[arg(help = "Number of search hits to return")]
        size: Option<u16>,
    },
}

pub struct Application {
    scheme: String,
    user: String,
    password: String,
    host: String,
    port: u16,
}

impl Application {
    fn uri(&self) -> String {
        format!(
            "{scheme}://{user}:{password}@{host}:{port}",
            scheme = self.scheme,
            user = self.user,
            password = self.password,
            host = self.host,
            port = self.port
        )
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = CommandLine::parse();
    let app = Application {
        scheme: String::from("http"),
        user: String::from("elastic"),
        password: String::from("nmd6NZXM"),
        host: String::from("localhost"),
        port: 9200,
    };
    let transport = Transport::single_node(&*app.uri())?;
    let es = Elasticsearch::new(transport);
    match &args.command {
        Commands::Info {} => {
            info(es).await?;
        }
        Commands::ListIndexes { all } => {
            list_indices(es, all).await?;
        }
        Commands::CreateIndex { index, mappings } => {
            create_index(es, index, mappings).await?;
        }
        Commands::DeleteIndex { index } => {
            delete_index(es, index).await?;
        }
        Commands::Load {
            index,
            csv_filenames,
        } => {
            load(es, index, csv_filenames).await?;
        }
        Commands::Search { index, size } => {
            search(es, index, size).await?;
        }
    }
    Ok(())
}

async fn info(es: Elasticsearch) -> Result<(), Box<dyn std::error::Error>> {
    let response = es.info().send().await?;
    let info = response.json::<Value>().await?;
    println!("Name: {}", info["name"]);
    println!("Cluster Name: {}", info["cluster_name"]);
    println!("Version: {}", info["version"]["number"]);
    println!("Tagline: {}", info["tagline"]);
    Ok(())
}

async fn list_indices(es: Elasticsearch, all: &bool) -> Result<(), Box<dyn std::error::Error>> {
    let response = es
        .indices()
        .get(IndicesGetParts::Index(&["*"]))
        .send()
        .await?;
    let index_list = response.json::<HashMap<String, Value>>().await?;
    for (key, _value) in index_list.into_iter() {
        if *all || !key.starts_with(".") {
            println!("{} {}", key, _value);
        }
    }
    Ok(())
}

async fn create_index(
    es: Elasticsearch,
    index: &str,
    mappings: &Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
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
    let response = es
        .indices()
        .create(IndicesCreateParts::Index(index))
        .body(body)
        .send()
        .await?;
    let created = response.json::<Value>().await?;
    println!("{}", created);
    Ok(())
}

async fn delete_index(es: Elasticsearch, index: &str) -> Result<(), Box<dyn std::error::Error>> {
    let response = es
        .indices()
        .delete(IndicesDeleteParts::Index(&[index]))
        .send()
        .await?;
    let deleted = response.json::<Value>().await?;
    println!("{}", deleted);
    Ok(())
}

async fn load(
    es: Elasticsearch,
    index: &str,
    csv_filenames: &Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
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
    let response = es
        .bulk(BulkParts::Index(index))
        .body(body)
        .refresh(Refresh::WaitFor)
        .send()
        .await?;
    let loaded = response.json::<Value>().await?;
    println!(
        "Loaded {} documents",
        loaded["items"].as_array().expect("No items loaded").len()
    );
    Ok(())
}

#[derive(Deserialize)]
struct SearchResult {
    hits: SearchResultHits,
}

#[derive(Deserialize)]
struct SearchResultHits {
    hits: Vec<Value>,
}

async fn search(
    es: Elasticsearch,
    index: &str,
    size: &Option<u16>,
) -> Result<(), Box<dyn std::error::Error>> {
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
    let response = es
        .search(SearchParts::Index(&[index]))
        .body(body)
        .send()
        .await?;
    let result = response.json::<SearchResult>().await?;
    for record in result.hits.hits.iter() {
        println!("{}", record);
    }
    Ok(())
}
