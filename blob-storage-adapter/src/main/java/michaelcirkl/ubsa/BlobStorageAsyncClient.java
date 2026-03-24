package michaelcirkl.ubsa;

import michaelcirkl.ubsa.client.pagination.ListingPage;
import michaelcirkl.ubsa.client.pagination.PagedFlowPublisher;
import michaelcirkl.ubsa.client.pagination.PageRequest;
import michaelcirkl.ubsa.client.streaming.BlobWriteOptions;

import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

public interface BlobStorageAsyncClient {
    Provider getProvider();

    <T> T unwrap(Class<T> nativeType);

    CompletableFuture<Boolean> bucketExists(String bucketName);

    CompletableFuture<Blob> getBlob(String bucketName, String blobKey);

    /**
     * Returns blob metadata without downloading content bytes. Implementations may return {@code null}
     * from {@link Blob#getContent()} for metadata-only reads.
     */
    default CompletableFuture<Blob> getBlobMetadata(String bucketName, String blobKey) {
        return getBlob(bucketName, blobKey).thenApply(BlobStorageAsyncClient::metadataOnly);
    }

    Flow.Publisher<ByteBuffer> openBlobStream(String bucketName, String blobKey);

    CompletableFuture<Void> deleteBucket(String bucketName);

    CompletableFuture<Boolean> blobExists(String bucketName, String blobKey);

    CompletableFuture<String> createBlob(String bucketName, Blob blob);

    CompletableFuture<String> createBlob(String bucketName, String blobKey, Path sourceFile);

    CompletableFuture<String> createBlob(String bucketName, String blobKey, Flow.Publisher<ByteBuffer> content, long contentLength, BlobWriteOptions options);

    CompletableFuture<Void> deleteBlobIfExists(String bucketName, String blobKey);

    CompletableFuture<String> copyBlob(
            String sourceBucketName,
            String sourceBlobKey,
            String destinationBucketName,
            String destinationBlobKey
    );

    CompletableFuture<ListingPage<Bucket>> listBuckets(PageRequest request);

    CompletableFuture<ListingPage<Blob>> listBlobs(String bucketName, String prefix, PageRequest request);

    CompletableFuture<List<Bucket>> listAllBuckets();

    default Flow.Publisher<Bucket> streamBuckets(int pageSize) {
        return new PagedFlowPublisher<>(PageRequest.builder().pageSize(pageSize).build(), this::listBuckets);
    }

    default Flow.Publisher<Blob> streamBlobs(String bucketName, String prefix, int pageSize) {
        return new PagedFlowPublisher<>(
                PageRequest.builder().pageSize(pageSize).build(),
                pageRequest -> listBlobs(bucketName, prefix, pageRequest)
        );
    }

    CompletableFuture<Void> createBucket(Bucket bucket);

    CompletableFuture<Void> deleteBucketIfExists(String bucketName);

    CompletableFuture<byte[]> getByteRange(String bucketName, String blobKey, long startInclusive, long endInclusive);

    URL generateGetUrl(String bucket, String objectKey, Duration expiry);

    URL generatePutUrl(String bucket, String objectKey, Duration expiry, String contentType);

    private static Blob metadataOnly(Blob blob) {
        if (blob == null) {
            return null;
        }
        return Blob.builder()
                .bucket(blob.getBucket())
                .key(blob.getKey())
                .size(blob.getSize())
                .lastModified(blob.lastModified())
                .encoding(blob.encoding())
                .etag(blob.getEtag())
                .userMetadata(blob.getUserMetadata())
                .publicURI(blob.getPublicURI())
                .expires(blob.expires())
                .build();
    }
}
