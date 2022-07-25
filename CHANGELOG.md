# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

### Changed

- Updated tests to be compatible with core Prefect library (v2.0b9) and bumped required version - [#38](https://github.com/PrefectHQ/prefect-azure/pull/38)
- Renamed `BlobStorageAzureCredentials` to `AzureBlobStorageCredentials`, `CosmosDbAzureCredentials` to `AzureCosmosDbCredentials`, and `MlAzureCredentials` to `AzureMlCredentials` - [#39](https://github.com/PrefectHQ/prefect-azure/pull/39)
- Converted `AzureBlobStorageCredentials`, `AzureCosmosDbCredentials`, `AzureMlCredentials` to Blocks - [#39](https://github.com/PrefectHQ/prefect-azure/pull/39)

### Deprecated

### Removed

### Fixed

### Security

## 0.1.0

Released on March 8th, 2022.

### Added

- `ml_upload_datastore`, `ml_get_datastore`, `ml_list_datastores`, and `ml_register_datastore_blob_container` tasks - [#15](https://github.com/PrefectHQ/prefect-azure/pull/15)
- `cosmos_db_query_items`, `cosmos_db_read_item`, and `cosmos_db_create_item` tasks - [#6](https://github.com/PrefectHQ/prefect-azure/pull/6)
- `blob_storage_download`, `blob_storage_upload`, and `blob_storage_list` tasks - [#4](https://github.com/PrefectHQ/prefect-azure/pull/4)
