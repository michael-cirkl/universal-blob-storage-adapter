package michaelcirkl.ubsa;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface BlobStorageClient {

    CompletableFuture<Boolean> bucketExists(String bucketName);

    CompletableFuture<Blob> getBlob(String bucketName, String blobName);

    CompletableFuture<Void> deleteBucket(String bucketName);

    CompletableFuture<Boolean> blobExists(String bucketName, String blobName);

    CompletableFuture<String> createBlob(String bucketName, Blob blob);

    CompletableFuture<Void> deleteBlob(String bucketName, String blobName);

    CompletableFuture<String> copyBlob(
            String sourceBucketName,
            String sourceBlobName,
            String destinationBucketName,
            String destinationBlobName
    );

    CompletableFuture<Set<Bucket>> listAllBuckets();

    CompletableFuture<Set<Blob>> listBlobsByPrefix(String bucketName, String prefix);

    CompletableFuture<Void> createBucket(String bucketName);

    CompletableFuture<Set<Blob>> getAllBlobsInBucket(String bucketName);

    CompletableFuture<Void> deleteBucketIfExists(String bucketName);

    CompletableFuture<byte[]> getByteRange(String bucketName, String blobName, long startInclusive, long endInclusive);

    CompletableFuture<String> createBlobIfNotExists(String bucketName, Blob blob);
}
