mod client;
mod viz;

use std::{
    collections::HashMap,
    env,
    error::Error,
    process::exit,
    thread::sleep,
    time::{Duration, SystemTime},
};

use clap::{Parser, Subcommand, ValueEnum};
use elasticsearch::{auth::Credentials, http::Url};
use serde_json::Value;

use client::{Es, EsBulkSummary, EsInfo, EsSearchResult};
use viz::DataTable;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct CommandLine {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    #[command(about = "Ping a HEAD request to the service root to check availability")]
    Ping {
        #[arg(short = 'c', long = "count")]
        #[arg(help = "Stop after sending COUNT requests")]
        count: Option<usize>,
        #[arg(short = 'i', long = "interval")]
        #[arg(help = "Time to wait in seconds between requests (default 1s)")]
        #[arg(default_value_t = 1.0)]
        interval: f64,
    },

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
        #[arg(help = "Lucene search query")]
        query: Option<String>,
        #[arg(short = 'o', long = "order-by")]
        #[arg(help = "Comma-separated list of FIELD:DIRECTION pairs")]
        order_by: Option<String>,
        #[arg(short = 'l', long = "limit")]
        #[arg(help = "Maximum number of search hits to return (default 10)")]
        limit: Option<u16>,
        #[arg(short = 'f', long = "format")]
        #[arg(help = "Output format for search results")]
        #[arg(default_value_t = SearchResultFormat::Table, value_enum)]
        format: SearchResultFormat,
    },
}

#[derive(ValueEnum, Copy, Clone, Debug, PartialEq, Eq)]
enum SearchResultFormat {
    Raw,
    Table,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // TODO: detect presence of start-local (look for .env file or check local ports)
    let args = CommandLine::parse();
    match env::var("ESCLI_URL") {
        // "http://localhost:9200"
        Ok(url) => {
            let url =
                Url::parse(url.as_str()).expect(format!("Failed to parse URL: {url}").as_str());
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
                    Err(_) => {
                        eprintln!("Please set Elasticsearch credentials with either ESCLI_API_KEY or ESCLI_USER/ESCLI_PASSWORD");
                        exit(1);
                    }
                },
            }
            let es = Es::new(url, auth);
            despatch(&args.command, &es).await?;
            Ok(())
        }
        Err(_) => {
            eprintln!("The ESCLI_URL environment variable is not set. Please set this with the URL of an Elasticsearch service.");
            exit(1);
        }
    }
}

async fn despatch(command: &Commands, es: &Es) -> Result<(), Box<dyn Error>> {
    match command {
        Commands::Ping { count, interval } => {
            println!("HEAD {}", es.url());
            let mut seq: usize = 0;
            loop {
                seq += 1;
                let t0 = SystemTime::now();
                let result = es.ping().await;
                let elapsed = t0.elapsed().expect("System time error");
                match result {
                    Ok(status_code) => {
                        println!("{status_code}: seq={seq} time={elapsed:?}");
                    }
                    Err(e) => {
                        println!("{e}: seq={seq} time={elapsed:?}");
                    }
                }
                if count.is_some_and(|x| seq >= x) {
                    break;
                }
                sleep(Duration::from_secs_f64(*interval));
            }
        }
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
            query,
            order_by,
            limit,
            format,
        } => {
            let result = &es.search(index, query, order_by, limit).await?;
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
    for (key, _value) in index_list.iter() {
        if *all || !key.starts_with('.') {
            println!("{} {}", key, _value);
        }
    }
}

fn print_bulk_summary(summary: &EsBulkSummary) {
    let mut results: HashMap<String, usize> = HashMap::new();
    for item in summary.items.iter() {
        for (_key, value) in item.iter() {
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
            for hit in result.hits.hits.iter() {
                println!("{:?}", hit);
            }
        }
        SearchResultFormat::Table => {
            let mut table = DataTable::new();
            for hit in result.hits.hits.iter() {
                table.push_document(&hit._source);
            }
            table.print();
        }
    }
}
