package michaelcirkl.ubsa;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface BlobStorageClient {
    Provider getProvider();

    CompletableFuture<Boolean> bucketExists(String bucketName);

    CompletableFuture<Blob> getBlob(String bucketName, String blobKey);

    CompletableFuture<Void> deleteBucket(String bucketName);

    CompletableFuture<Boolean> blobExists(String bucketName, String blobKey);

    CompletableFuture<String> createBlob(String bucketName, Blob blob);

    CompletableFuture<Void> deleteBlob(String bucketName, String blobKey);

    CompletableFuture<String> copyBlob(
            String sourceBucketName,
            String sourceBlobKey,
            String destinationBucketName,
            String destinationBlobKey
    );

    CompletableFuture<Set<Bucket>> listAllBuckets();

    CompletableFuture<Set<Blob>> listBlobsByPrefix(String bucketName, String prefix);

    CompletableFuture<Void> createBucket(Bucket bucket);

    CompletableFuture<Set<Blob>> getAllBlobsInBucket(String bucketName);

    CompletableFuture<Void> deleteBucketIfExists(String bucketName);

    CompletableFuture<byte[]> getByteRange(String bucketName, String blobKey, long startInclusive, long endInclusive);

    CompletableFuture<String> createBlobIfNotExists(String bucketName, Blob blob);
}

