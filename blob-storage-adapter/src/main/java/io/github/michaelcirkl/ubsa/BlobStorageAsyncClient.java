package io.github.michaelcirkl.ubsa;

import io.github.michaelcirkl.ubsa.client.streaming.BlobWriteOptions;
import io.github.michaelcirkl.ubsa.client.pagination.ListingPage;
import io.github.michaelcirkl.ubsa.client.pagination.PagedFlowPublisher;
import io.github.michaelcirkl.ubsa.client.pagination.PageRequest;

import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

/**
 * Asynchronous provider-neutral client for bucket and blob operations.
 *
 * <p>This interface exposes a common async API across AWS S3, Azure Blob Storage, and Google Cloud Storage.
 */
public interface BlobStorageAsyncClient {
    /**
     * Returns the underlying cloud provider for this client.
     */
    Provider getProvider();

    /**
     * Returns the native SDK client when it matches the requested type.
     *
     * <p>This can be used to access provider-specific features that are outside the UBSA abstraction.
     * <p>Supported client types are AWS {@code S3AsyncClient}, Azure {@code BlobServiceAsyncClient},
     * and GCP {@code Storage}.
     *
     * @return the native client, or {@code null} when the requested type does not match
     */
    <T> T unwrap(Class<T> nativeType);

    /**
     * Returns whether the bucket/container exists.
     */
    CompletableFuture<Boolean> bucketExists(String bucketName);

    /**
     * Returns the blob content and metadata.
     */
    CompletableFuture<Blob> getBlob(String bucketName, String blobKey);

    /**
     * Returns blob metadata without loading the content into memory.
     *
     * <p>The returned {@link Blob} has {@code null} content.
     */
    CompletableFuture<Blob> getBlobMetadata(String bucketName, String blobKey);

    /**
     * Opens a publisher that emits the blob content as byte buffers.
     */
    Flow.Publisher<ByteBuffer> openBlobStream(String bucketName, String blobKey);

    /**
     * Deletes the bucket/container.
     */
    CompletableFuture<Void> deleteBucket(String bucketName);

    /**
     * Returns whether the blob exists.
     */
    CompletableFuture<Boolean> blobExists(String bucketName, String blobKey);

    /**
     * Creates or overwrites a blob using the key and content stored in the given {@link Blob}.
     *
     * <p>If {@link Blob#getContent()} is {@code null}, an empty blob is created.
     */
    CompletableFuture<String> createBlob(String bucketName, Blob blob);

    /**
     * Creates or overwrites a blob from a local file.
     */
    CompletableFuture<String> createBlob(String bucketName, String blobKey, Path sourceFile);

    /**
     * Creates or overwrites a blob from a local file, applying optional write metadata such as
     * content encoding and user metadata.
     */
    CompletableFuture<String> createBlob(String bucketName, String blobKey, Path sourceFile, BlobWriteOptions options);

    /**
     * Creates or overwrites a blob from a publisher with a known content length.
     */
    CompletableFuture<String> createBlob(String bucketName, String blobKey, Flow.Publisher<ByteBuffer> content, long contentLength, BlobWriteOptions options);

    /**
     * Deletes the blob when it exists and does not fail when it is already missing.
     */
    CompletableFuture<Void> deleteBlobIfExists(String bucketName, String blobKey);

    /**
     * Copies a blob to another bucket/key and returns the ETag of the new blob when the provider exposes one.
     */
    CompletableFuture<String> copyBlob(
            String sourceBucketName,
            String sourceBlobKey,
            String destinationBucketName,
            String destinationBlobKey
    );

    /**
     * Returns a single page of buckets/containers.
     *
     * <p>If {@code request} is {@code null}, the first page is requested with provider defaults.
     */
    CompletableFuture<ListingPage<Bucket>> listBuckets(PageRequest request);

    /**
     * Returns a single page of blobs in the given bucket/container.
     *
     * <p>If {@code prefix} is non-blank, only matching blob keys are returned. If {@code request} is
     * {@code null}, the first page is requested with provider defaults.
     */
    CompletableFuture<ListingPage<Blob>> listBlobs(String bucketName, String prefix, PageRequest request);

    /**
     * Returns all buckets/containers by repeatedly loading pages until exhaustion.
     */
    CompletableFuture<List<Bucket>> listAllBuckets();

    /**
     * Streams all buckets/containers using paginated listing requests.
     */
    default Flow.Publisher<Bucket> streamBuckets(int pageSize) {
        return new PagedFlowPublisher<>(PageRequest.builder().pageSize(pageSize).build(), this::listBuckets);
    }

    /**
     * Streams all blobs matching the given prefix using paginated listing requests.
     */
    default Flow.Publisher<Blob> streamBlobs(String bucketName, String prefix, int pageSize) {
        return new PagedFlowPublisher<>(
                PageRequest.builder().pageSize(pageSize).build(),
                pageRequest -> listBlobs(bucketName, prefix, pageRequest)
        );
    }

    /**
     * Creates a bucket/container.
     */
    CompletableFuture<Void> createBucket(Bucket bucket);

    /**
     * Deletes the bucket/container when it exists and does not fail when it is already missing.
     */
    CompletableFuture<Void> deleteBucketIfExists(String bucketName);

    /**
     * Returns the inclusive byte range from the blob content.
     */
    CompletableFuture<byte[]> getByteRange(String bucketName, String blobKey, long startInclusive, long endInclusive);

    /**
     * Generates a temporary URL for downloading a blob.
     */
    URL generateGetUrl(String bucket, String objectKey, Duration expiry);

    /**
     * Generates a temporary URL for uploading a blob.
     */
    URL generatePutUrl(String bucket, String objectKey, Duration expiry);
}
