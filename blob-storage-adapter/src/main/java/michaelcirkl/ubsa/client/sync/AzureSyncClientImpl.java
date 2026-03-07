package michaelcirkl.ubsa.client.sync;

import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.*;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import michaelcirkl.ubsa.*;
import michaelcirkl.ubsa.client.streaming.BlobWriteOptions;
import michaelcirkl.ubsa.client.streaming.ContentLengthValidators;
import michaelcirkl.ubsa.client.streaming.WriteOptionsMappers;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
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
    public <T> T unwrap(Class<T> nativeType) {
        if (nativeType == null) {
            throw new IllegalArgumentException("Class type to unwrap must not be null.");
        }
        return nativeType.isInstance(client) ? nativeType.cast(client) : null;
    }

    @Override
    public Boolean bucketExists(String bucketName) {
        try {
            return client.getBlobContainerClient(bucketName).exists();
        } catch (BlobStorageException error) {
            throw new UbsaException("Failed to check whether Azure container exists: " + bucketName, error);
        }
    }

    @Override
    public Blob getBlob(String bucketName, String blobKey) {
        try {
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
        } catch (BlobStorageException error) {
            throw new UbsaException("Failed to get Azure blob " + blobKey + " from container " + bucketName, error);
        }
    }

    @Override
    public InputStream openBlobStream(String bucketName, String blobKey) {
        try {
            return blobClient(bucketName, blobKey).openInputStream();
        } catch (BlobStorageException error) {
            throw new UbsaException("Failed to open Azure blob stream " + bucketName + "/" + blobKey, error);
        }
    }

    @Override
    public Void deleteBucket(String bucketName) {
        try {
            client.getBlobContainerClient(bucketName).delete();
            return null;
        } catch (BlobStorageException error) {
            throw new UbsaException("Failed to delete Azure container: " + bucketName, error);
        }
    }

    @Override
    public Boolean blobExists(String bucketName, String blobKey) {
        try {
            return blobClient(bucketName, blobKey).exists();
        } catch (BlobStorageException error) {
            throw new UbsaException(
                    "Failed to check whether Azure blob exists: " + bucketName + "/" + blobKey,
                    error
            );
        }
    }

    @Override
    public String createBlob(String bucketName, Blob blob) {
        try {
            BlobClient blobClient = blobClient(bucketName, blob.getKey());
            BlobParallelUploadOptions uploadOptions = WriteOptionsMappers.buildAzureUploadOptions(blob);
            return blobClient.uploadWithResponse(uploadOptions, null, null)
                    .getValue()
                    .getETag();
        } catch (BlobStorageException error) {
            throw new UbsaException(
                    "Failed to create Azure blob " + blob.getKey() + " in container " + bucketName,
                    error
            );
        }
    }

    @Override
    public String createBlob(String bucketName, String blobKey, InputStream content, long contentLength, BlobWriteOptions options) {
        ContentLengthValidators.validateContentLength(contentLength);
        if (content == null) {
            throw new IllegalArgumentException("Content stream must not be null.");
        }
        try {
            BlobClient blobClient = blobClient(bucketName, blobKey);
            BlobHttpHeaders headers = WriteOptionsMappers.toAzureHeaders(options);
            Map<String, String> metadata = WriteOptionsMappers.toAzureMetadata(options);
            BlobParallelUploadOptions uploadOptions = new BlobParallelUploadOptions(BinaryData.fromStream(content, contentLength))
                    .setHeaders(headers)
                    .setMetadata(metadata);
            blobClient.uploadWithResponse(uploadOptions, null, Context.NONE);
            return blobClient.getProperties().getETag();
        } catch (BlobStorageException error) {
            throw new UbsaException(
                    "Failed to stream-create Azure blob " + blobKey + " in container " + bucketName,
                    error
            );
        }
    }

    @Override
    public Void deleteBlob(String bucketName, String blobKey) {
        try {
            blobClient(bucketName, blobKey).deleteIfExists();
            return null;
        } catch (BlobStorageException error) {
            throw new UbsaException("Failed to delete Azure blob " + bucketName + "/" + blobKey, error);
        }
    }

    @Override
    public String copyBlob(String sourceBucketName, String sourceBlobKey, String destinationBucketName, String destinationBlobKey) {
        try {
            BlobClient sourceBlobClient = blobClient(sourceBucketName, sourceBlobKey);
            BlobClient destinationBlobClient = blobClient(destinationBucketName, destinationBlobKey);
            destinationBlobClient.copyFromUrl(sourceBlobClient.getBlobUrl());
            return destinationBlobClient.getProperties().getETag();
        } catch (BlobStorageException error) {
            throw new UbsaException(
                    "Failed to copy Azure blob from " + sourceBucketName + "/" + sourceBlobKey
                            + " to " + destinationBucketName + "/" + destinationBlobKey,
                    error
            );
        }
    }

    @Override
    public Set<Bucket> listAllBuckets() {
        try {
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
        } catch (BlobStorageException error) {
            throw new UbsaException("Failed to list Azure containers", error);
        }
    }

    @Override
    public Set<Blob> listBlobsByPrefix(String bucketName, String prefix) {
        try {
            BlobContainerClient containerClient = client.getBlobContainerClient(bucketName);
            ListBlobsOptions options = new ListBlobsOptions();
            if (prefix != null && !prefix.isBlank()) {
                options.setPrefix(prefix);
            }

            return mapBlobsFromList(bucketName, containerClient, containerClient.listBlobs(options, null));
        } catch (BlobStorageException error) {
            throw new UbsaException("Failed to list Azure blobs in container " + bucketName, error);
        }
    }

    @Override
    public Void createBucket(Bucket bucket) {
        try {
            client.createBlobContainer(bucket.getName());
            return null;
        } catch (BlobStorageException error) {
            throw new UbsaException("Failed to create Azure container " + bucket.getName(), error);
        }
    }

    @Override
    public Set<Blob> getAllBlobsInBucket(String bucketName) {
        return listBlobsByPrefix(bucketName, null);
    }

    @Override
    public Void deleteBucketIfExists(String bucketName) {
        try {
            client.getBlobContainerClient(bucketName).deleteIfExists();
            return null;
        } catch (BlobStorageException error) {
            throw new UbsaException("Failed to delete Azure container if exists: " + bucketName, error);
        }
    }

    @Override
    public byte[] getByteRange(String bucketName, String blobKey, long startInclusive, long endInclusive) {
        validateRange(startInclusive, endInclusive);
        try {
            BlobRange blobRange = new BlobRange(startInclusive, endInclusive - startInclusive + 1);
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            blobClient(bucketName, blobKey)
                    .downloadStreamWithResponse(output, blobRange, null, null, false, null, null);
            return output.toByteArray();
        } catch (BlobStorageException error) {
            throw new UbsaException(
                    "Failed to read byte range from Azure blob " + bucketName + "/" + blobKey,
                    error
            );
        }
    }

    @Override
    public String createBlobIfNotExists(String bucketName, Blob blob) {
        BlobClient blobClient = blobClient(bucketName, blob.getKey());
        try {
            BlobParallelUploadOptions uploadOptions = WriteOptionsMappers.buildAzureUploadOptions(blob)
                    .setRequestConditions(new BlobRequestConditions().setIfNoneMatch("*"));
            return blobClient.uploadWithResponse(uploadOptions, null, null)
                    .getValue()
                    .getETag();
        } catch (BlobStorageException error) {
            if (isPreconditionConflict(error)) {
                try {
                    return blobClient.getProperties().getETag();
                } catch (BlobStorageException readError) {
                    throw new UbsaException(
                            "Failed to read existing Azure blob after conditional create conflict: "
                                    + bucketName + "/" + blob.getKey(),
                            readError
                    );
                }
            }
            throw new UbsaException(
                    "Failed to create Azure blob if not exists: " + bucketName + "/" + blob.getKey(),
                    error
            );
        }
    }

    @Override
    public URL generateGetUrl(String bucket, String objectKey, Duration expiry) {
        validateExpiry(expiry);
        try {
            var blobClient = client.getBlobContainerClient(bucket).getBlobClient(objectKey);
            BlobSasPermission permission = new BlobSasPermission().setReadPermission(true);
            BlobServiceSasSignatureValues values = new BlobServiceSasSignatureValues(OffsetDateTime.now().plus(expiry), permission);
            String sas = blobClient.generateSas(values);
            return buildSasUrl(blobClient.getBlobUrl(), sas);
        } catch (BlobStorageException error) {
            throw new UbsaException("Failed to generate Azure GET URL for " + bucket + "/" + objectKey, error);
        }
    }

    @Override
    public URL generatePutUrl(String bucket, String objectKey, Duration expiry, String contentType) {
        validateExpiry(expiry);
        try {
            var blobClient = client.getBlobContainerClient(bucket).getBlobClient(objectKey);
            BlobSasPermission permission = new BlobSasPermission()
                    .setCreatePermission(true)
                    .setWritePermission(true);
            BlobServiceSasSignatureValues values = new BlobServiceSasSignatureValues(OffsetDateTime.now().plus(expiry), permission);
            String sas = blobClient.generateSas(values);
            return buildSasUrl(blobClient.getBlobUrl(), sas);
        } catch (BlobStorageException error) {
            throw new UbsaException("Failed to generate Azure PUT URL for " + bucket + "/" + objectKey, error);
        }
    }

    private BlobClient blobClient(String bucketName, String blobKey) {
        return client.getBlobContainerClient(bucketName).getBlobClient(blobKey);
    }

    private Set<Blob> mapBlobsFromList(String bucketName, BlobContainerClient containerClient, Iterable<BlobItem> blobItems) {
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

    private LocalDateTime toLocalDateTime(OffsetDateTime time) {
        return time == null ? null : time.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime();
    }

    private static URI toUri(String uriValue) {
        return URI.create(uriValue);
    }

    private URL buildSasUrl(String baseUrl, String sasToken) {
        try {
            return URI.create(baseUrl + "?" + sasToken).toURL();
        } catch (IllegalArgumentException | MalformedURLException e) {
            throw new IllegalStateException("Failed to build SAS URL for Azure Blob Storage.", e);
        }
    }

    private void validateExpiry(Duration expiry) {
        if (expiry == null || expiry.isZero() || expiry.isNegative()) {
            throw new IllegalArgumentException("Expiry must be a positive duration.");
        }
    }

    private void validateRange(long startInclusive, long endInclusive) {
        if (startInclusive < 0 || endInclusive < startInclusive) {
            throw new IllegalArgumentException("Invalid range. startInclusive must be >= 0 and endInclusive must be >= startInclusive.");
        }
    }

    private boolean isPreconditionConflict(BlobStorageException error) {
        int statusCode = error.getStatusCode();
        if (statusCode == 412) {
            return true;
        }
        BlobErrorCode errorCode = error.getErrorCode();
        return errorCode == BlobErrorCode.BLOB_ALREADY_EXISTS
                || errorCode == BlobErrorCode.RESOURCE_ALREADY_EXISTS
                || errorCode == BlobErrorCode.CONDITION_NOT_MET
                || errorCode == BlobErrorCode.TARGET_CONDITION_NOT_MET;
    }

}
