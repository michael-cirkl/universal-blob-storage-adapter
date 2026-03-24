package michaelcirkl.ubsa;

import michaelcirkl.ubsa.client.pagination.ListingPage;
import michaelcirkl.ubsa.client.pagination.PagedFlowPublisher;
import michaelcirkl.ubsa.client.pagination.PageRequest;
import michaelcirkl.ubsa.client.streaming.BlobWriteOptions;

import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

public interface BlobStorageAsyncClient {
    Provider getProvider();

    <T> T unwrap(Class<T> nativeType);

    CompletableFuture<Boolean> bucketExists(String bucketName);

    CompletableFuture<Blob> getBlob(String bucketName, String blobKey);

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

}
