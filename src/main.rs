mod client;
mod tables;

use std::{collections::HashMap, error::Error};

use clap::{Parser, Subcommand, ValueEnum};
use serde_json::Value;
use tabled::settings::Style;

use client::{Es, EsBulkSummary, EsInfo, EsSearchResult};
use tables::TabularData;

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
        #[arg(help = "Lucene search query")]
        query: Option<String>,
        #[arg(short = 'l', long = "limit")]
        #[arg(help = "Maximum number of search hits to return")]
        size: Option<u16>,
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
    let uri = "http://elastic:nmd6NZXM@localhost:9200";
    let args = CommandLine::parse();
    let es = Es::new(uri);
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
            query,
            size,
            format,
        } => {
            let result = &es.search(index, query, size).await?;
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
            for hit in result.hits.hits.iter() {
                println!("{:?}", hit);
            }
        }
        SearchResultFormat::Table => {
            let mut data = TabularData::new();
            for hit in result.hits.hits.iter() {
                data.push_row(&hit._source);
            }
            println!("{}", data.to_table().with(Style::sharp()));
        }
    }
}
