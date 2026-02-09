package michaelcirkl.ubsa.client.sync;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.BlobStorageSyncClient;
import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.Provider;

import java.net.URI;
import java.net.MalformedURLException;
import java.net.URL;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Map;
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
        return client.getBlobContainerClient(bucketName).exists();
    }

    @Override
    public Blob getBlob(String bucketName, String blobKey) {
        BlobClient blobClient = blobClient(bucketName, blobKey);
        BlobProperties properties = blobClient.getProperties();
        BinaryData content = blobClient.downloadContent();

        return Blob.builder()
                .bucket(bucketName)
                .key(blobKey)
                .content(content.toBytes())
                .size(properties.getBlobSize())
                .lastModified(toLocalDateTime(properties.getLastModified()))
                .encoding(properties.getContentEncoding())
                .etag(properties.getETag())
                .userMetadata(properties.getMetadata())
                .publicURI(toUri(blobClient.getBlobUrl()))
                .expires(toLocalDateTime(properties.getExpiresOn()))
                .build();
    }

    @Override
    public Void deleteBucket(String bucketName) {
        client.getBlobContainerClient(bucketName).delete();
        return null;
    }

    @Override
    public Boolean blobExists(String bucketName, String blobKey) {
        return blobClient(bucketName, blobKey).exists();
    }

    @Override
    public String createBlob(String bucketName, Blob blob) {
        BlobClient blobClient = blobClient(bucketName, blob.getKey());
        BlobParallelUploadOptions uploadOptions = buildUploadOptions(blob);
        return blobClient.uploadWithResponse(uploadOptions, null, null)
                .getValue()
                .getETag();
    }

    @Override
    public Void deleteBlob(String bucketName, String blobKey) {
        blobClient(bucketName, blobKey).deleteIfExists();
        return null;
    }

    @Override
    public String copyBlob(String sourceBucketName, String sourceBlobKey, String destinationBucketName, String destinationBlobKey) {
        BlobClient sourceBlobClient = blobClient(sourceBucketName, sourceBlobKey);
        BlobClient destinationBlobClient = blobClient(destinationBucketName, destinationBlobKey);
        destinationBlobClient.copyFromUrl(sourceBlobClient.getBlobUrl());
        return destinationBlobClient.getProperties().getETag();
    }

    @Override
    public Set<Bucket> listAllBuckets() {
        Set<Bucket> buckets = new HashSet<>();
        client.listBlobContainers().forEach(item -> {
            OffsetDateTime lastModified = item.getProperties() == null
                    ? null
                    : item.getProperties().getLastModified();
            buckets.add(Bucket.builder()
                    .name(item.getName())
                    .publicURI(toUri(client.getBlobContainerClient(item.getName()).getBlobContainerUrl()))
                    .creationDate(toLocalDateTime(lastModified))
                    .lastModified(toLocalDateTime(lastModified))
                    .build());
        });
        return buckets;
    }

    @Override
    public Set<Blob> listBlobsByPrefix(String bucketName, String prefix) {
        BlobContainerClient containerClient = client.getBlobContainerClient(bucketName);
        ListBlobsOptions options = new ListBlobsOptions();
        if (prefix != null && !prefix.isBlank()) {
            options.setPrefix(prefix);
        }

        return mapBlobsFromList(bucketName, containerClient, containerClient.listBlobs(options, null));
    }

    @Override
    public Void createBucket(Bucket bucket) {
        client.createBlobContainer(bucket.getName());
        return null;
    }

    @Override
    public Set<Blob> getAllBlobsInBucket(String bucketName) {
        return listBlobsByPrefix(bucketName, null);
    }

    @Override
    public Void deleteBucketIfExists(String bucketName) {
        client.getBlobContainerClient(bucketName).deleteIfExists();
        return null;
    }

    @Override
    public byte[] getByteRange(String bucketName, String blobKey, long startInclusive, long endInclusive) {
        validateRange(startInclusive, endInclusive);
        BlobRange blobRange = new BlobRange(startInclusive, endInclusive - startInclusive + 1);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        blobClient(bucketName, blobKey)
                .downloadStreamWithResponse(output, blobRange, null, null, false, null, null);
        return output.toByteArray();
    }

    @Override
    public String createBlobIfNotExists(String bucketName, Blob blob) {
        BlobClient blobClient = blobClient(bucketName, blob.getKey());
        if (blobClient.exists()) {
            return blobClient.getProperties().getETag();
        }
        return blobClient.uploadWithResponse(buildUploadOptions(blob), null, null)
                .getValue()
                .getETag();
    }

    @Override
    public URL generateGetUrl(String bucket, String objectKey, Duration expiry) {
        validateExpiry(expiry);
        var blobClient = client.getBlobContainerClient(bucket).getBlobClient(objectKey);
        BlobSasPermission permission = new BlobSasPermission().setReadPermission(true);
        BlobServiceSasSignatureValues values = new BlobServiceSasSignatureValues(OffsetDateTime.now().plus(expiry), permission);
        String sas = blobClient.generateSas(values);
        return buildSasUrl(blobClient.getBlobUrl(), sas);
    }

    @Override
    public URL generatePutUrl(String bucket, String objectKey, Duration expiry, String contentType) {
        validateExpiry(expiry);
        var blobClient = client.getBlobContainerClient(bucket).getBlobClient(objectKey);
        BlobSasPermission permission = new BlobSasPermission()
                .setCreatePermission(true)
                .setWritePermission(true);
        BlobServiceSasSignatureValues values = new BlobServiceSasSignatureValues(OffsetDateTime.now().plus(expiry), permission);
        String sas = blobClient.generateSas(values);
        return buildSasUrl(blobClient.getBlobUrl(), sas);
    }

    private BlobClient blobClient(String bucketName, String blobKey) {
        return client.getBlobContainerClient(bucketName).getBlobClient(blobKey);
    }

    private static BlobParallelUploadOptions buildUploadOptions(Blob blob) {
        byte[] content = blob.getContent() == null ? new byte[0] : blob.getContent();
        BlobParallelUploadOptions uploadOptions = new BlobParallelUploadOptions(BinaryData.fromBytes(content));

        BlobHttpHeaders headers = new BlobHttpHeaders();
        boolean hasHeaders = false;
        if (blob.encoding() != null && !blob.encoding().isBlank()) {
            headers.setContentEncoding(blob.encoding());
            hasHeaders = true;
        }
        if (hasHeaders) {
            uploadOptions.setHeaders(headers);
        }

        Map<String, String> metadata = blob.getUserMetadata();
        if (metadata != null && !metadata.isEmpty()) {
            uploadOptions.setMetadata(metadata);
        }

        return uploadOptions;
    }

    private static Set<Blob> mapBlobsFromList(String bucketName, BlobContainerClient containerClient, Iterable<BlobItem> blobItems) {
        Set<Blob> blobs = new HashSet<>();
        blobItems.forEach(item -> {
            BlobItemProperties properties = item.getProperties();
            long size = properties != null && properties.getContentLength() != null
                    ? properties.getContentLength()
                    : 0L;
            blobs.add(Blob.builder()
                    .bucket(bucketName)
                    .key(item.getName())
                    .size(size)
                    .lastModified(toLocalDateTime(properties == null ? null : properties.getLastModified()))
                    .encoding(properties == null ? null : properties.getContentEncoding())
                    .etag(properties == null ? null : properties.getETag())
                    .userMetadata(item.getMetadata())
                    .publicURI(toUri(containerClient.getBlobClient(item.getName()).getBlobUrl()))
                    .expires(toLocalDateTime(properties == null ? null : properties.getExpiryTime()))
                    .build());
        });
        return blobs;
    }

    private static LocalDateTime toLocalDateTime(OffsetDateTime time) {
        return time == null ? null : time.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime();
    }

    private static URI toUri(String uriValue) {
        return URI.create(uriValue);
    }

    private static URL buildSasUrl(String baseUrl, String sasToken) {
        try {
            return new URL(baseUrl + "?" + sasToken);
        } catch (MalformedURLException e) {
            throw new IllegalStateException("Failed to build SAS URL for Azure Blob Storage.", e);
        }
    }

    private static void validateExpiry(Duration expiry) {
        if (expiry == null || expiry.isZero() || expiry.isNegative()) {
            throw new IllegalArgumentException("Expiry must be a positive duration.");
        }
    }

    private static void validateRange(long startInclusive, long endInclusive) {
        if (startInclusive < 0 || endInclusive < startInclusive) {
            throw new IllegalArgumentException("Invalid range. startInclusive must be >= 0 and endInclusive must be >= startInclusive.");
        }
    }
}
