package michaelcirkl.ubsa.impl.async;

import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.BlobStorageAsyncClient;
import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.Provider;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.time.OffsetDateTime;
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

    @Override
    public CompletableFuture<URL> generateGetUrl(String bucket, String objectKey, Duration expiry) {
        var blobClient = client.getBlobContainerAsyncClient(bucket).getBlobAsyncClient(objectKey);
        BlobSasPermission permission = new BlobSasPermission()
                .setReadPermission(true);
        BlobServiceSasSignatureValues values = new BlobServiceSasSignatureValues(OffsetDateTime.now().plus(expiry), permission);
        String sas = blobClient.generateSas(values);
        return CompletableFuture.completedFuture(buildSasUrl(blobClient.getBlobUrl(), sas));
    }

    @Override
    public CompletableFuture<URL> generatePutUrl(String bucket, String objectKey, Duration expiry, String contentType) {
        var blobClient = client.getBlobContainerAsyncClient(bucket).getBlobAsyncClient(objectKey);
        BlobSasPermission permission = new BlobSasPermission()
                .setCreatePermission(true)
                .setWritePermission(true);
        BlobServiceSasSignatureValues values = new BlobServiceSasSignatureValues(OffsetDateTime.now().plus(expiry), permission);
        String sas = blobClient.generateSas(values);
        return CompletableFuture.completedFuture(buildSasUrl(blobClient.getBlobUrl(), sas));
    }

    private static URL buildSasUrl(String baseUrl, String sasToken) {
        try {
            return new URL(baseUrl + "?" + sasToken);
        } catch (MalformedURLException e) {
            throw new IllegalStateException("Failed to build SAS URL for Azure Blob Storage.", e);
        }
    }
}
