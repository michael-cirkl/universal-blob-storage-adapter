package michaelcirkl.ubsa.client.async;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.BlobStorageAsyncClient;
import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.Provider;
import michaelcirkl.ubsa.UbsaException;

import java.net.URI;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

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
    public <T> T unwrap(Class<T> nativeType) {
        if (nativeType == null) {
            throw new IllegalArgumentException("Class type to unwrap must not be null.");
        }
        return nativeType.isInstance(client) ? nativeType.cast(client) : null;
    }

    @Override
    public CompletableFuture<Boolean> bucketExists(String bucketName) {
        return wrapBlobStorageException(
                client.getBlobContainerAsyncClient(bucketName)
                        .exists()
                        .toFuture(),
                "Failed to check whether Azure container exists: " + bucketName
        );
    }

    @Override
    public CompletableFuture<Blob> getBlob(String bucketName, String blobKey) {
        BlobAsyncClient blobClient = blobClient(bucketName, blobKey);
        CompletableFuture<BlobProperties> propertiesFuture = blobClient.getProperties().toFuture();
        CompletableFuture<BinaryData> contentFuture = blobClient.downloadContent().toFuture();

        return wrapBlobStorageException(
                propertiesFuture.thenCombine(contentFuture, (properties, content) -> Blob.builder()
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
                        .build()),
                "Failed to get Azure blob " + bucketName + "/" + blobKey
        );
    }

    @Override
    public CompletableFuture<Void> deleteBucket(String bucketName) {
        return wrapBlobStorageException(
                client.getBlobContainerAsyncClient(bucketName)
                        .delete()
                        .toFuture(),
                "Failed to delete Azure container: " + bucketName
        );
    }

    @Override
    public CompletableFuture<Boolean> blobExists(String bucketName, String blobKey) {
        return wrapBlobStorageException(
                blobClient(bucketName, blobKey)
                        .exists()
                        .toFuture(),
                "Failed to check whether Azure blob exists: " + bucketName + "/" + blobKey
        );
    }

    @Override
    public CompletableFuture<String> createBlob(String bucketName, Blob blob) {
        BlobAsyncClient blobClient = blobClient(bucketName, blob.getKey());
        BlobParallelUploadOptions uploadOptions = buildUploadOptions(blob);
        return wrapBlobStorageException(
                blobClient.uploadWithResponse(uploadOptions)
                        .map(response -> response.getValue().getETag())
                        .toFuture(),
                "Failed to create Azure blob " + bucketName + "/" + blob.getKey()
        );
    }

    @Override
    public CompletableFuture<Void> deleteBlob(String bucketName, String blobKey) {
        return wrapBlobStorageException(
                blobClient(bucketName, blobKey)
                        .deleteIfExists()
                        .then()
                        .toFuture(),
                "Failed to delete Azure blob " + bucketName + "/" + blobKey
        );
    }

    @Override
    public CompletableFuture<String> copyBlob(String sourceBucketName, String sourceBlobKey, String destinationBucketName, String destinationBlobKey) {
        BlobAsyncClient sourceBlobClient = blobClient(sourceBucketName, sourceBlobKey);
        BlobAsyncClient destinationBlobClient = blobClient(destinationBucketName, destinationBlobKey);
        return wrapBlobStorageException(
                destinationBlobClient.copyFromUrl(sourceBlobClient.getBlobUrl())
                        .flatMap(copyId -> destinationBlobClient.getProperties().map(BlobProperties::getETag))
                        .toFuture(),
                "Failed to copy Azure blob from " + sourceBucketName + "/" + sourceBlobKey
                        + " to " + destinationBucketName + "/" + destinationBlobKey
        );
    }

    @Override
    public CompletableFuture<Set<Bucket>> listAllBuckets() {
        return wrapBlobStorageException(
                client.listBlobContainers()
                        .collectList()
                        .map(containerItems -> {
                            Set<Bucket> buckets = new HashSet<>();
                            containerItems.forEach(item -> {
                                OffsetDateTime lastModified = item.getProperties() == null
                                        ? null
                                        : item.getProperties().getLastModified();
                                buckets.add(Bucket.builder()
                                        .name(item.getName())
                                        .publicURI(toUri(client.getBlobContainerAsyncClient(item.getName()).getBlobContainerUrl()))
                                        .creationDate(toLocalDateTime(lastModified))
                                        .lastModified(toLocalDateTime(lastModified))
                                        .build());
                            });
                            return buckets;
                        })
                        .toFuture(),
                "Failed to list Azure containers"
        );
    }

    @Override
    public CompletableFuture<Set<Blob>> listBlobsByPrefix(String bucketName, String prefix) {
        BlobContainerAsyncClient containerClient = client.getBlobContainerAsyncClient(bucketName);
        ListBlobsOptions options = new ListBlobsOptions();
        if (prefix != null && !prefix.isBlank()) {
            options.setPrefix(prefix);
        }

        return wrapBlobStorageException(
                containerClient.listBlobs(options, null)
                        .collectList()
                        .map(blobItems -> mapBlobsFromList(bucketName, containerClient, blobItems))
                        .toFuture(),
                "Failed to list Azure blobs in container " + bucketName
        );
    }

    @Override
    public CompletableFuture<Void> createBucket(Bucket bucket) {
        return wrapBlobStorageException(
                client.createBlobContainer(bucket.getName())
                        .then()
                        .toFuture(),
                "Failed to create Azure container " + bucket.getName()
        );
    }

    @Override
    public CompletableFuture<Set<Blob>> getAllBlobsInBucket(String bucketName) {
        return listBlobsByPrefix(bucketName, null);
    }

    @Override
    public CompletableFuture<Void> deleteBucketIfExists(String bucketName) {
        return wrapBlobStorageException(
                client.getBlobContainerAsyncClient(bucketName)
                        .deleteIfExists()
                        .then()
                        .toFuture(),
                "Failed to delete Azure container if exists: " + bucketName
        );
    }

    @Override
    public CompletableFuture<byte[]> getByteRange(String bucketName, String blobKey, long startInclusive, long endInclusive) {
        validateRange(startInclusive, endInclusive);
        BlobRange blobRange = new BlobRange(startInclusive, endInclusive - startInclusive + 1);

        return wrapBlobStorageException(
                blobClient(bucketName, blobKey)
                        .downloadStreamWithResponse(blobRange, null, null, false)
                        .flatMap(response -> BinaryData.fromFlux(response.getValue()))
                        .map(BinaryData::toBytes)
                        .toFuture(),
                "Failed to read byte range from Azure blob " + bucketName + "/" + blobKey
        );
    }

    @Override
    public CompletableFuture<String> createBlobIfNotExists(String bucketName, Blob blob) {
        BlobAsyncClient blobClient = blobClient(bucketName, blob.getKey());
        return wrapBlobStorageException(
                blobClient.exists()
                        .flatMap(exists -> {
                            if (exists) {
                                return blobClient.getProperties().map(BlobProperties::getETag);
                            }
                            return blobClient.uploadWithResponse(buildUploadOptions(blob))
                                    .map(response -> response.getValue().getETag());
                        })
                        .toFuture(),
                "Failed to create Azure blob if not exists: " + bucketName + "/" + blob.getKey()
        );
    }

    @Override
    public CompletableFuture<URL> generateGetUrl(String bucket, String objectKey, Duration expiry) {
        validateExpiry(expiry);
        try {
            var blobClient = client.getBlobContainerAsyncClient(bucket).getBlobAsyncClient(objectKey);
            BlobSasPermission permission = new BlobSasPermission()
                    .setReadPermission(true);
            BlobServiceSasSignatureValues values = new BlobServiceSasSignatureValues(OffsetDateTime.now().plus(expiry), permission);
            String sas = blobClient.generateSas(values);
            return CompletableFuture.completedFuture(buildSasUrl(blobClient.getBlobUrl(), sas));
        } catch (BlobStorageException error) {
            throw new UbsaException("Failed to generate Azure GET URL for " + bucket + "/" + objectKey, error);
        }
    }

    @Override
    public CompletableFuture<URL> generatePutUrl(String bucket, String objectKey, Duration expiry, String contentType) {
        validateExpiry(expiry);
        try {
            var blobClient = client.getBlobContainerAsyncClient(bucket).getBlobAsyncClient(objectKey);
            BlobSasPermission permission = new BlobSasPermission()
                    .setCreatePermission(true)
                    .setWritePermission(true);
            BlobServiceSasSignatureValues values = new BlobServiceSasSignatureValues(OffsetDateTime.now().plus(expiry), permission);
            String sas = blobClient.generateSas(values);
            return CompletableFuture.completedFuture(buildSasUrl(blobClient.getBlobUrl(), sas));
        } catch (BlobStorageException error) {
            throw new UbsaException("Failed to generate Azure PUT URL for " + bucket + "/" + objectKey, error);
        }
    }

    private BlobAsyncClient blobClient(String bucketName, String blobKey) {
        return client.getBlobContainerAsyncClient(bucketName).getBlobAsyncClient(blobKey);
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

    private static Set<Blob> mapBlobsFromList(String bucketName, BlobContainerAsyncClient containerClient, java.util.List<BlobItem> blobItems) {
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
                    .publicURI(toUri(containerClient.getBlobAsyncClient(item.getName()).getBlobUrl()))
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

    private static <T> CompletableFuture<T> wrapBlobStorageException(CompletableFuture<T> future, String message) {
        return future.handle((result, error) -> {
            if (error == null) {
                return result;
            }
            Throwable cause = unwrapCompletionException(error);
            throw toCompletionException(message, cause);
        });
    }

    private static CompletionException toCompletionException(String message, Throwable cause) {
        if (cause instanceof UbsaException ubsaException) {
            return new CompletionException(ubsaException);
        }
        if (cause instanceof BlobStorageException blobStorageException) {
            return new CompletionException(new UbsaException(message, blobStorageException));
        }
        return new CompletionException(cause);
    }

    private static Throwable unwrapCompletionException(Throwable error) {
        if (error instanceof CompletionException completionException && completionException.getCause() != null) {
            return completionException.getCause();
        }
        return error;
    }
}
