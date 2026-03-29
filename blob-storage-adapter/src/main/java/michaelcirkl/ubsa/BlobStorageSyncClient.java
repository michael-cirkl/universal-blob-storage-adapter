package michaelcirkl.ubsa;

import michaelcirkl.ubsa.client.pagination.ListingPage;
import michaelcirkl.ubsa.client.pagination.PagedIterable;
import michaelcirkl.ubsa.client.pagination.PageRequest;
import michaelcirkl.ubsa.client.streaming.BlobWriteOptions;

import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

/**
 * Synchronous provider-neutral client for bucket and blob operations.
 *
 * <p>This interface exposes a common API across AWS S3, Azure Blob Storage, and Google Cloud Storage.
 */
public interface BlobStorageSyncClient {
    /**
     * Returns the underlying cloud provider for this client.
     */
    Provider getProvider();

    /**
     * Returns the native SDK client when it matches the requested type.
     *
     * <p>This can be used to access provider-specific features that are outside the UBSA abstraction.
     *
     * @return the native client, or {@code null} when the requested type does not match
     */
    <T> T unwrap(Class<T> nativeType);

    /**
     * Returns whether the bucket/container exists.
     */
    Boolean bucketExists(String bucketName);

    /**
     * Returns the blob content and metadata.
     */
    Blob getBlob(String bucketName, String blobKey);

    /**
     * Returns blob metadata without loading the content into memory.
     *
     * <p>The returned {@link Blob} has {@code null} content.
     */
    Blob getBlobMetadata(String bucketName, String blobKey);

    /**
     * Opens a streaming read for the blob content.
     *
     * <p>The caller is responsible for closing the returned {@link InputStream}.
     */
    InputStream openBlobStream(String bucketName, String blobKey);

    /**
     * Deletes the bucket/container.
     */
    Void deleteBucket(String bucketName);

    /**
     * Returns whether the blob exists.
     */
    Boolean blobExists(String bucketName, String blobKey);

    /**
     * Creates or overwrites a blob using the key and content stored in the given {@link Blob}.
     *
     * <p>If {@link Blob#getContent()} is {@code null}, an empty blob is created.
     */
    String createBlob(String bucketName, Blob blob);

    /**
     * Creates or overwrites a blob from a local file.
     */
    String createBlob(String bucketName, String blobKey, Path sourceFile);

    /**
     * Creates or overwrites a blob from a local file, applying optional write metadata such as
     * content encoding and user metadata.
     */
    String createBlob(String bucketName, String blobKey, Path sourceFile, BlobWriteOptions options);

    /**
     * Creates or overwrites a blob from an input stream with a known content length.
     */
    String createBlob(String bucketName, String blobKey, InputStream content, long contentLength, BlobWriteOptions options);

    /**
     * Deletes the blob when it exists and does not fail when it is already missing.
     */
    Void deleteBlobIfExists(String bucketName, String blobKey);

    /**
     * Copies a blob to another bucket/key and returns the ETag of the new blob when the provider exposes one.
     */
    String copyBlob(
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
    ListingPage<Bucket> listBuckets(PageRequest request);

    /**
     * Returns a single page of blobs in the given bucket/container.
     *
     * <p>If {@code prefix} is non-blank, only matching blob keys are returned. If {@code request} is
     * {@code null}, the first page is requested with provider defaults.
     */
    ListingPage<Blob> listBlobs(String bucketName, String prefix, PageRequest request);

    /**
     * Returns all buckets/containers by repeatedly loading pages until exhaustion.
     */
    List<Bucket> listAllBuckets();

    /**
     * Lazily iterates across all buckets/containers using paginated listing requests.
     */
    default Iterable<Bucket> iterateBuckets(int pageSize) {
        return new PagedIterable<>(PageRequest.builder().pageSize(pageSize).build(), this::listBuckets);
    }

    /**
     * Lazily iterates across all blobs matching the given prefix using paginated listing requests.
     */
    default Iterable<Blob> iterateBlobs(String bucketName, String prefix, int pageSize) {
        return new PagedIterable<>(
                PageRequest.builder().pageSize(pageSize).build(),
                pageRequest -> listBlobs(bucketName, prefix, pageRequest)
        );
    }

    /**
     * Creates a bucket/container.
     */
    Void createBucket(Bucket bucket);

    /**
     * Deletes the bucket/container when it exists and does not fail when it is already missing.
     */
    Void deleteBucketIfExists(String bucketName);

    /**
     * Returns the inclusive byte range from the blob content.
     */
    byte[] getByteRange(String bucketName, String blobKey, long startInclusive, long endInclusive);

    /**
     * Generates a temporary URL for downloading a blob.
     */
    URL generateGetUrl(String bucket, String objectKey, Duration expiry);

    /**
     * Generates a temporary URL for uploading a blob.
     *
     * <p>{@code contentType} is optional and is used only by providers that support constraining the signed request.
     * Azure ignores this parameter because its generated SAS upload URL does not bind the content type.
     */
    URL generatePutUrl(String bucket, String objectKey, Duration expiry, String contentType);
}
