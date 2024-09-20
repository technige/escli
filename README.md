# Escli

Escli is a command line tool for Elasticsearch.
It has been rebuilt in 2024 in Rust, following an earlier prototype in 2021 that used Python.
This allows for greater portability, and also makes use of the offical Rust client due for GA release in 2025.


## Scope

The tool is not intended to provide access to the full Elasticsearch API.
Instead, it offers a curated set of established and heavily-used functionality which is guaranteed to be available across all Elasticsearch offerings (serverless, cloud, on-prem).

Specifically, the following areas of functionality are included:
- Search
- Bulk ingestion
- Document management
- Index management
- Informational
