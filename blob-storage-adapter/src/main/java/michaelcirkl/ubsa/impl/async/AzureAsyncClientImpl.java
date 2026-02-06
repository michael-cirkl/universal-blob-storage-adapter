package michaelcirkl.ubsa.impl.async;

import com.azure.storage.blob.BlobServiceAsyncClient;
import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.BlobStorageAsyncClient;
import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.Provider;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class AzureAsyncClientImpl implements BlobStorageAsyncClient {
    private final BlobServiceAsyncClient client;

    public AzureAsyncClientImpl(BlobServiceAsyncClient client) {
        this.client = client;
    }

    @Override
    public Provider getProvider() {
        return Provider.Azure;
    }

    @Override
    public CompletableFuture<Boolean> bucketExists(String bucketName) {
        return null;
    }

    @Override
    public CompletableFuture<Blob> getBlob(String bucketName, String blobKey) {
        return null;
    }

    @Override
    public CompletableFuture<Void> deleteBucket(String bucketName) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> blobExists(String bucketName, String blobKey) {
        return null;
    }

    @Override
    public CompletableFuture<String> createBlob(String bucketName, Blob blob) {
        return null;
    }

    @Override
    public CompletableFuture<Void> deleteBlob(String bucketName, String blobKey) {
        return null;
    }

    @Override
    public CompletableFuture<String> copyBlob(String sourceBucketName, String sourceBlobKey, String destinationBucketName, String destinationBlobKey) {
        return null;
    }

    @Override
    public CompletableFuture<Set<Bucket>> listAllBuckets() {
        return null;
    }

    @Override
    public CompletableFuture<Set<Blob>> listBlobsByPrefix(String bucketName, String prefix) {
        return null;
    }

    @Override
    public CompletableFuture<Void> createBucket(Bucket bucket) {
        return null;
    }

    @Override
    public CompletableFuture<Set<Blob>> getAllBlobsInBucket(String bucketName) {
        return null;
    }

    @Override
    public CompletableFuture<Void> deleteBucketIfExists(String bucketName) {
        return null;
    }

    @Override
    public CompletableFuture<byte[]> getByteRange(String bucketName, String blobKey, long startInclusive, long endInclusive) {
        return null;
    }

    @Override
    public CompletableFuture<String> createBlobIfNotExists(String bucketName, Blob blob) {
        return null;
    }
}
