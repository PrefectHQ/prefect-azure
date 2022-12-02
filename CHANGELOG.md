# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- Added flow run cancellation support to the `AzureContainerInstanceJob` block - [#58](https://github.com/PrefectHQ/prefect-azure/pull/58)
- Added Managed Identities and custom DNS servers to `AzureContainerInstanceJob` - [#60](https://github.com/PrefectHQ/prefect-azure/pull/60)

### Changed

- Updated the Container Instance Job block to treat `PREFECT_API_KEY` as a secure environment variable - [#57](https://github.com/PrefectHQ/prefect-azure/pull/57)

### Deprecated

### Removed

### Fixed

- Fixed handling of private Docker image registries - [#54](https://github.com/PrefectHQ/prefect-azure/pull/54)

### Security

## 0.2.2

Released on October 20th, 2022.

### Changed

- Made `AzureContainerInstanceJob` accessible directly from the `prefect_azure` module - [#50](https://github.com/PrefectHQ/prefect-azure/pull/50)

## 0.2.1

Released on October 20th, 2022.

### Added

- `AzureContainerInstanceJob` infrastructure block - [#45](https://github.com/PrefectHQ/prefect-azure/pull/45)

## 0.2.0

Released on July 26th, 2022.

### Changed

- Updated tests to be compatible with core Prefect library (v2.0b9) and bumped required version - [#38](https://github.com/PrefectHQ/prefect-azure/pull/38)
- Renamed `BlobStorageAzureCredentials` to `AzureBlobStorageCredentials`, `CosmosDbAzureCredentials` to `AzureCosmosDbCredentials`, and `MlAzureCredentials` to `AzureMlCredentials` - [#39](https://github.com/PrefectHQ/prefect-azure/pull/39)
- Converted `AzureBlobStorageCredentials`, `AzureCosmosDbCredentials`, `AzureMlCredentials` to Blocks - [#39](https://github.com/PrefectHQ/prefect-azure/pull/39)

## 0.1.0

Released on March 8th, 2022.

### Added

- `ml_upload_datastore`, `ml_get_datastore`, `ml_list_datastores`, and `ml_register_datastore_blob_container` tasks - [#15](https://github.com/PrefectHQ/prefect-azure/pull/15)
- `cosmos_db_query_items`, `cosmos_db_read_item`, and `cosmos_db_create_item` tasks - [#6](https://github.com/PrefectHQ/prefect-azure/pull/6)
- `blob_storage_download`, `blob_storage_upload`, and `blob_storage_list` tasks - [#4](https://github.com/PrefectHQ/prefect-azure/pull/4)
