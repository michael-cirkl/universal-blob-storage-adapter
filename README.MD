# Universal Blob Storage Adapter (UBSA)
*Write once, use any object storage provider.*

UBSA is a Java library that provides a unified async client for object storage services including S3, Azure Blob Storage, and Google Cloud Storage.

It offers a single, consistent API across providers, making it easy to switch backends without rewriting your application code.

## Why UBSA?
Migrating object storage across providers is painful:
- Different SDKs and APIs
- Inconsistent exception handling
- Vendor lock-in


UBSA solves this with a two-step approach:

1. **Migrate in increments**: incrementally rewrite your storage code to use the UBSA API
2. **Switch anytime**: change the underlying provider via configuration

This means that after the initial migration, switching between S3, Azure Blob Storage, or GCS no longer requires
code changes—only passing a different client to the factory.

## Features

- 🌐 Multi-provider support (S3, Azure Blob Storage, GCS, S3-compatible)
- ⚡ Native async and sync clients
- 🔌 Built on top of official SDKs
- 📦 Simple, consistent API
- 🚀 Streaming support, pre-signed URLs, and access to native clients

## Example
```java
S3Client nativeClient;
// BlobServiceAsyncClient nativeClient;

BlobStorageAsyncClient client = BlobStorageClientFactory.getAsyncClient(nativeClient);

// Same API regardless of provider
client.createBucket(...);
client.createBlob(...);
client.getBlob(...);
```

## Documentation
Library is documented on wiki: https://github.com/michael-cirkl/universal-blob-storage-adapter/wiki

Maven Central download link: https://central.sonatype.com/artifact/io.github.michael-cirkl/ubsa/overview