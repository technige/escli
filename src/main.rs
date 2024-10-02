mod client;
mod data;

use std::{
    collections::HashMap,
    process::{exit, ExitCode},
    thread::sleep,
    time::{Duration, SystemTime},
};

use byte_unit::{Byte, UnitType};
use clap::{Parser, Subcommand, ValueEnum};

use client::{RawBulkSummary, RawSearchResult, SimpleClient};
use data::Table;
use tabled::settings::{object::Columns, Alignment, Padding, Style};

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
    ListIndexes {
        #[arg(short = 'a', long = "all")]
        #[arg(help = "Match any data stream or index, including hidden ones")]
        all: bool,
        #[arg(short = 'o', long = "open")]
        #[arg(help = "Match open, non-hidden indices (also matches any non-hidden data stream)")]
        open: bool,
        #[arg(short = 'c', long = "closed")]
        #[arg(help = "Match closed, non-hidden indices (also matches any non-hidden data stream)")]
        closed: bool,
        #[arg(help = "Index name or pattern to include in list")]
        index: Option<String>,
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
async fn main() -> ExitCode {
    let args = CommandLine::parse();
    match SimpleClient::default() {
        Ok(es) => despatch(&args.command, &es).await,
        Err(e) => {
            eprintln!("{}", e);
            ExitCode::FAILURE
        }
    }
}

async fn despatch(command: &Commands, es: &SimpleClient) -> ExitCode {
    match command {
        Commands::Ping { count, interval } => ping(es, count, interval).await,
        Commands::Info {} => print_info(es).await,
        Commands::ListIndexes {
            index,
            all,
            open,
            closed,
        } => print_index_list(es, index, *all, *open, *closed).await,
        Commands::CreateIndex { index, mappings } => {
            match &es.create_index(index, mappings).await {
                Ok(created) => {
                    println!(
                        "Created index {} ({}acknowledged)",
                        created.index,
                        if created.acknowledged { "" } else { "not " }
                    );
                }
                Err(error) => {
                    eprintln!("{}", error);
                    exit(1);
                }
            };
            ExitCode::SUCCESS
        }
        Commands::DeleteIndex { index } => {
            match &es.delete_index(index).await {
                Ok(deleted) => {
                    println!(
                        "Deleted index ({}acknowledged)",
                        if deleted.acknowledged { "" } else { "not " }
                    );
                }
                Err(error) => {
                    eprintln!("{}", error);
                    exit(1);
                }
            }
            ExitCode::SUCCESS
        }
        Commands::Load {
            index,
            csv_filenames,
        } => {
            let summary = &match es.load(index, csv_filenames).await {
                Ok(it) => it,
                Err(e) => {
                    eprintln!("{}", e);
                    return ExitCode::FAILURE;
                }
            };
            print_bulk_summary(summary);
            ExitCode::SUCCESS
        }
        Commands::Search {
            index,
            query,
            order_by,
            limit,
            format,
        } => {
            let result = &match es.search(index, query, order_by, limit).await {
                Ok(it) => it,
                Err(e) => {
                    eprintln!("{}", e);
                    exit(1);
                }
            };
            print_search_result(result, format);
            ExitCode::SUCCESS
        }
    }
}

async fn ping(es: &SimpleClient, count: &Option<usize>, interval: &f64) -> ExitCode {
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
    ExitCode::SUCCESS
}

async fn print_info(es: &SimpleClient) -> ExitCode {
    match es.info().await {
        Ok(info) => {
            println!("Name: {}", info.name);
            println!("Cluster Name: {}", info.cluster_name);
            println!("Cluster UUID: {}", info.cluster_uuid);
            println!("Version:");
            println!("  Number: {}", info.version.number);
            println!("  Build Flavor: {}", info.version.build_flavor);
            println!("  Build Type: {}", info.version.build_type);
            println!("  Build Hash: {}", info.version.build_hash);
            println!("  Build Date: {}", info.version.build_date);
            println!("  Build Snapshot: {}", info.version.build_snapshot);
            println!("  Lucene Version: {}", info.version.lucene_version);
            println!(
                "  Minimum Wire Compatibility Version: {}",
                info.version.minimum_wire_compatibility_version
            );
            println!(
                "  Minimum Index Compatibility Version: {}",
                info.version.minimum_index_compatibility_version
            );
            println!("Tagline: {}", info.tagline);
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("{}", e);
            ExitCode::FAILURE
        }
    }
}

async fn print_index_list(
    es: &SimpleClient,
    index: &Option<String>,
    all: bool,
    open: bool,
    closed: bool,
) -> ExitCode {
    match es
        .get_index_list(
            &[index.clone().unwrap_or(String::from("*")).as_str()],
            all,
            open,
            closed,
        )
        .await
    {
        Ok(index_list) => {
            let mut builder = tabled::builder::Builder::default();
            let mut has_rows = false;
            for entry in index_list.iter() {
                if all || !entry.name.starts_with('.') {
                    builder.push_record(vec![
                        match entry.health.as_str() {
                            "green" => "🟢",
                            "yellow" => "🟡",
                            "red" => "🔴",
                            _ => "⚫",
                        },
                        &entry.uuid,
                        &entry.name,
                        &format!("{} docs", entry.docs_count.unwrap_or(0),),
                        &format!(
                            "{:-#.1}",
                            Byte::from_u64(entry.dataset_size.unwrap_or(0))
                                .get_appropriate_unit(UnitType::Decimal)
                        ),
                        match entry.status.as_str() {
                            "closed" => "🔒",
                            _ => "",
                        },
                    ]);
                    has_rows = true;
                }
            }
            if has_rows {
                println!(
                    "{}",
                    builder
                        .build()
                        .with(Style::empty())
                        .modify(Columns::first(), Padding::new(0, 1, 0, 0))
                        .modify(Columns::single(3), Alignment::right())
                        .modify(Columns::single(4), Alignment::right())
                );
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("{}", e);
            ExitCode::FAILURE
        }
    }
}

fn print_bulk_summary(summary: &RawBulkSummary) {
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

fn print_search_result(result: &RawSearchResult, format: &SearchResultFormat) {
    match format {
        SearchResultFormat::Raw => {
            for hit in result.hits.hits.iter() {
                println!("{:?}", hit);
            }
        }
        SearchResultFormat::Table => {
            let mut table = Table::new();
            for hit in result.hits.hits.iter() {
                table.push_document(&hit._source);
            }
            if table.count_rows() == 0 {
                println!("No rows")
            } else {
                table.print();
            }
        }
    }
}
