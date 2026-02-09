package michaelcirkl.ubsa.client.async;


import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.HttpMethod;
import com.google.cloud.storage.Storage;
import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.BlobStorageAsyncClient;
import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.Provider;

import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class GCPAsyncClientImpl implements BlobStorageAsyncClient {
    private final Storage client;

    public GCPAsyncClientImpl(Storage client) {
        this.client = client;
        //check if client has gRPC
    }

    @Override
    public Provider getProvider() {
        return Provider.GCP;
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

    @Override
    public CompletableFuture<URL> generateGetUrl(String bucket, String objectKey, Duration expiry) {
        long seconds = toPositiveSeconds(expiry);
        BlobInfo blobInfo = BlobInfo.newBuilder(bucket, objectKey).build();
        URL url = client.signUrl(blobInfo, seconds, TimeUnit.SECONDS);
        return CompletableFuture.completedFuture(url);
    }

    @Override
    public CompletableFuture<URL> generatePutUrl(String bucket, String objectKey, Duration expiry, String contentType) {
        long seconds = toPositiveSeconds(expiry);
        BlobInfo.Builder blobBuilder = BlobInfo.newBuilder(bucket, objectKey);
        if (contentType != null && !contentType.isBlank()) {
            blobBuilder.setContentType(contentType);
        }
        BlobInfo blobInfo = blobBuilder.build();
        var options = new ArrayList<Storage.SignUrlOption>();
        options.add(Storage.SignUrlOption.httpMethod(HttpMethod.PUT));
        if (contentType != null && !contentType.isBlank()) {
            options.add(Storage.SignUrlOption.withContentType());
        }
        URL url = client.signUrl(blobInfo, seconds, TimeUnit.SECONDS, options.toArray(new Storage.SignUrlOption[0]));
        return CompletableFuture.completedFuture(url);
    }

    private static long toPositiveSeconds(Duration expiry) {
        if (expiry == null || expiry.isZero() || expiry.isNegative()) {
            throw new IllegalArgumentException("Expiry must be a positive duration.");
        }
        return expiry.toSeconds();
    }
}
