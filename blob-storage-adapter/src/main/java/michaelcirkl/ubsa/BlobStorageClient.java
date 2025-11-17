package michaelcirkl.ubsa;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface BlobStorageClient {

    CompletableFuture<Boolean> bucketExists();

    CompletableFuture<Blob> getBlob();

    CompletableFuture<Void> deleteBucket();

    CompletableFuture<Boolean> blobExists();

    CompletableFuture<String> createBlob();

    CompletableFuture<Void> deleteBlob();

    CompletableFuture<String> copyBlob();

    CompletableFuture<Set<Bucket>> listAllBuckets();

    CompletableFuture<Set<Blob>> listBlobsByPrefix();

    CompletableFuture<Void> createBucket();

    CompletableFuture<Set<Blob>> getAllBlobsInBucket();

    CompletableFuture<Void> deleteBucketIfExists();

    CompletableFuture<byte[]> getByteRange();

    CompletableFuture<String> createBlobIfNotExists();





    CompletableFuture<Void> startUpload();

    CompletableFuture<Void> uploadPart();

    CompletableFuture<Void> uploadComplete();

    CompletableFuture<Set<String>> uploadsList();

    CompletableFuture<Void> uploadAbort();
}
