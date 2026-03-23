package michaelcirkl.ubsa;

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

    CompletableFuture<Flow.Publisher<ByteBuffer>> openBlobStream(String bucketName, String blobKey);

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

    CompletableFuture<List<Bucket>> listAllBuckets();

    CompletableFuture<List<Blob>> listBlobsByPrefix(String bucketName, String prefix);

    CompletableFuture<Void> createBucket(Bucket bucket);

    CompletableFuture<List<Blob>> getAllBlobsInBucket(String bucketName);

    CompletableFuture<Void> deleteBucketIfExists(String bucketName);

    CompletableFuture<byte[]> getByteRange(String bucketName, String blobKey, long startInclusive, long endInclusive);

    URL generateGetUrl(String bucket, String objectKey, Duration expiry);

    URL generatePutUrl(String bucket, String objectKey, Duration expiry, String contentType);

}
