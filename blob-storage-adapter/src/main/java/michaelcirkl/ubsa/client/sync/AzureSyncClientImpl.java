package michaelcirkl.ubsa.client.sync;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.BlobStorageSyncClient;
import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.Provider;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Set;

public class AzureSyncClientImpl implements BlobStorageSyncClient {
    private final BlobServiceClient client;

    public AzureSyncClientImpl(BlobServiceClient client) {
        this.client = client;
    }

    @Override
    public Provider getProvider() {
        return Provider.Azure;
    }

    @Override
    public Boolean bucketExists(String bucketName) {
        return null;
    }

    @Override
    public Blob getBlob(String bucketName, String blobKey) {
        return null;
    }

    @Override
    public Void deleteBucket(String bucketName) {
        return null;
    }

    @Override
    public Boolean blobExists(String bucketName, String blobKey) {
        return null;
    }

    @Override
    public String createBlob(String bucketName, Blob blob) {
        return null;
    }

    @Override
    public Void deleteBlob(String bucketName, String blobKey) {
        return null;
    }

    @Override
    public String copyBlob(String sourceBucketName, String sourceBlobKey, String destinationBucketName, String destinationBlobKey) {
        return null;
    }

    @Override
    public Set<Bucket> listAllBuckets() {
        return null;
    }

    @Override
    public Set<Blob> listBlobsByPrefix(String bucketName, String prefix) {
        return null;
    }

    @Override
    public Void createBucket(Bucket bucket) {
        return null;
    }

    @Override
    public Set<Blob> getAllBlobsInBucket(String bucketName) {
        return null;
    }

    @Override
    public Void deleteBucketIfExists(String bucketName) {
        return null;
    }

    @Override
    public byte[] getByteRange(String bucketName, String blobKey, long startInclusive, long endInclusive) {
        return new byte[0];
    }

    @Override
    public String createBlobIfNotExists(String bucketName, Blob blob) {
        return null;
    }

    @Override
    public URL generateGetUrl(String bucket, String objectKey, Duration expiry) {
        var blobClient = client.getBlobContainerClient(bucket).getBlobClient(objectKey);
        BlobSasPermission permission = new BlobSasPermission().setReadPermission(true);
        BlobServiceSasSignatureValues values = new BlobServiceSasSignatureValues(OffsetDateTime.now().plus(expiry), permission);
        String sas = blobClient.generateSas(values);
        return buildSasUrl(blobClient.getBlobUrl(), sas);
    }

    @Override
    public URL generatePutUrl(String bucket, String objectKey, Duration expiry, String contentType) {
        var blobClient = client.getBlobContainerClient(bucket).getBlobClient(objectKey);
        BlobSasPermission permission = new BlobSasPermission()
                .setCreatePermission(true)
                .setWritePermission(true);
        BlobServiceSasSignatureValues values = new BlobServiceSasSignatureValues(OffsetDateTime.now().plus(expiry), permission);
        String sas = blobClient.generateSas(values);
        return buildSasUrl(blobClient.getBlobUrl(), sas);
    }

    private static URL buildSasUrl(String baseUrl, String sasToken) {
        try {
            return new URL(baseUrl + "?" + sasToken);
        } catch (MalformedURLException e) {
            throw new IllegalStateException("Failed to build SAS URL for Azure Blob Storage.", e);
        }
    }
}
