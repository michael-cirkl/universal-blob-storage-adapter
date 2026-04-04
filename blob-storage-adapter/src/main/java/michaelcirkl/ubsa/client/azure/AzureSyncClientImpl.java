package michaelcirkl.ubsa.client.azure;

import com.azure.core.http.rest.PagedResponse;
import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.*;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.blob.options.BlobUploadFromFileOptions;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import michaelcirkl.ubsa.*;
import michaelcirkl.ubsa.client.exception.AzureExceptionHandler;
import michaelcirkl.ubsa.client.pagination.BucketListingSupport;
import michaelcirkl.ubsa.client.pagination.ListingPage;
import michaelcirkl.ubsa.client.pagination.PageRequest;
import michaelcirkl.ubsa.client.streaming.BlobWriteOptions;
import michaelcirkl.ubsa.client.streaming.ByteArrayRangeValidator;
import michaelcirkl.ubsa.client.streaming.ContentLengthValidators;
import michaelcirkl.ubsa.client.streaming.FileUploadValidators;
import michaelcirkl.ubsa.client.streaming.WriteOptionsMappers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class AzureSyncClientImpl implements BlobStorageSyncClient {
    private final AzureExceptionHandler exceptionHandler = new AzureExceptionHandler();
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
        return exceptionHandler.handle(() -> client.getBlobContainerClient(bucketName).exists());
    }

    @Override
    public Blob getBlob(String bucketName, String blobKey) {
        return exceptionHandler.handle(() -> {
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
        });
    }

    @Override
    public Blob getBlobMetadata(String bucketName, String blobKey) {
        return exceptionHandler.handle(() -> {
            BlobClient blobClient = blobClient(bucketName, blobKey);
            BlobProperties properties = blobClient.getProperties();

            return Blob.builder()
                    .bucket(bucketName)
                    .key(blobKey)
                    .size(properties.getBlobSize())
                    .lastModified(toLocalDateTime(properties.getLastModified()))
                    .encoding(properties.getContentEncoding())
                    .etag(properties.getETag())
                    .userMetadata(properties.getMetadata())
                    .publicURI(toUri(blobClient.getBlobUrl()))
                    .expires(toLocalDateTime(properties.getExpiresOn()))
                    .build();
        });
    }

    @Override
    public InputStream openBlobStream(String bucketName, String blobKey) {
        return exceptionHandler.handle(() -> blobClient(bucketName, blobKey).openInputStream());
    }

    @Override
    public Void deleteBucket(String bucketName) {
        return exceptionHandler.handle(() -> {
            client.getBlobContainerClient(bucketName).delete();
            return null;
        });
    }

    @Override
    public Boolean blobExists(String bucketName, String blobKey) {
        return exceptionHandler.handle(() -> blobClient(bucketName, blobKey).exists());
    }

    @Override
    public String createBlob(String bucketName, Blob blob) {
        return exceptionHandler.handle(() -> {
            BlobClient blobClient = blobClient(bucketName, blob.getKey());
            BlobParallelUploadOptions uploadOptions = WriteOptionsMappers.buildAzureUploadOptions(blob);
            return blobClient.uploadWithResponse(uploadOptions, null, null)
                    .getValue()
                    .getETag();
        });
    }

    @Override
    public String createBlob(String bucketName, String blobKey, Path sourceFile) {
        return createBlob(bucketName, blobKey, sourceFile, null);
    }

    @Override
    public String createBlob(String bucketName, String blobKey, Path sourceFile, BlobWriteOptions options) {
        FileUploadValidators.validateSourceFile(sourceFile);
        return exceptionHandler.handle(() -> {
            BlobClient blobClient = blobClient(bucketName, blobKey);
            BlobHttpHeaders headers = WriteOptionsMappers.toAzureHeaders(options);
            Map<String, String> metadata = WriteOptionsMappers.toAzureMetadata(options);
            BlobUploadFromFileOptions uploadOptions = new BlobUploadFromFileOptions(sourceFile.toString())
                    .setHeaders(headers)
                    .setMetadata(metadata);
            return blobClient.uploadFromFileWithResponse(uploadOptions, null, Context.NONE)
                    .getValue()
                    .getETag();
        });
    }

    @Override
    public String createBlob(String bucketName, String blobKey, InputStream content, long contentLength, BlobWriteOptions options) {
        ContentLengthValidators.validateContentLength(contentLength);
        if (content == null) {
            throw new IllegalArgumentException("Content stream must not be null.");
        }
        return exceptionHandler.handle(() -> {
            BlobClient blobClient = blobClient(bucketName, blobKey);
            BlobHttpHeaders headers = WriteOptionsMappers.toAzureHeaders(options);
            Map<String, String> metadata = WriteOptionsMappers.toAzureMetadata(options);
            ByteArrayOutputStream bufferedContent = new ByteArrayOutputStream();
            var channel = Channels.newChannel(bufferedContent);
            ContentLengthValidators.copyInputStreamToChannel(content, channel, contentLength);
            BlobParallelUploadOptions uploadOptions = new BlobParallelUploadOptions(BinaryData.fromBytes(bufferedContent.toByteArray()))
                    .setHeaders(headers)
                    .setMetadata(metadata);
            return blobClient.uploadWithResponse(uploadOptions, null, Context.NONE)
                    .getValue()
                    .getETag();
        });
    }

    @Override
    public Void deleteBlobIfExists(String bucketName, String blobKey) {
        return exceptionHandler.handle(() -> {
            blobClient(bucketName, blobKey).deleteIfExists();
            return null;
        });
    }

    @Override
    public String copyBlob(String sourceBucketName, String sourceBlobKey, String destinationBucketName, String destinationBlobKey) {
        return exceptionHandler.handle(() -> {
            BlobClient sourceBlobClient = blobClient(sourceBucketName, sourceBlobKey);
            BlobClient destinationBlobClient = blobClient(destinationBucketName, destinationBlobKey);
            destinationBlobClient.copyFromUrl(sourceBlobClient.getBlobUrl());
            return destinationBlobClient.getProperties().getETag();
        });
    }

    @Override
    public ListingPage<Bucket> listBuckets(PageRequest request) {
        PageRequest pageRequest = normalizePageRequest(request);
        return exceptionHandler.handle(() -> {
            Iterator<PagedResponse<BlobContainerItem>> pages = pageRequest.getPageSize() == null
                    ? client.listBlobContainers().iterableByPage(pageRequest.getContinuationToken()).iterator()
                    : client.listBlobContainers().iterableByPage(pageRequest.getContinuationToken(), pageRequest.getPageSize()).iterator();
            if (!pages.hasNext()) {
                return ListingPage.of(List.of(), null);
            }

            PagedResponse<BlobContainerItem> page = pages.next();
            return ListingPage.of(mapBuckets(page.getElements()), page.getContinuationToken());
        });
    }

    @Override
    public ListingPage<Blob> listBlobs(String bucketName, String prefix, PageRequest request) {
        PageRequest pageRequest = normalizePageRequest(request);
        return exceptionHandler.handle(() -> {
            BlobContainerClient containerClient = client.getBlobContainerClient(bucketName);
            ListBlobsOptions options = new ListBlobsOptions();
            options.setDetails(new BlobListDetails().setRetrieveMetadata(true));
            if (prefix != null && !prefix.isBlank()) {
                options.setPrefix(prefix);
            }
            Iterator<PagedResponse<BlobItem>> pages = pageRequest.getPageSize() == null
                    ? containerClient.listBlobs(options, null).iterableByPage(pageRequest.getContinuationToken()).iterator()
                    : containerClient.listBlobs(options, null).iterableByPage(pageRequest.getContinuationToken(), pageRequest.getPageSize()).iterator();
            if (!pages.hasNext()) {
                return ListingPage.of(List.of(), null);
            }

            PagedResponse<BlobItem> page = pages.next();
            return ListingPage.of(mapBlobsFromList(bucketName, containerClient, page.getElements()), page.getContinuationToken());
        });
    }

    @Override
    public List<Bucket> listAllBuckets() {
        return BucketListingSupport.listAllBuckets(this::listBuckets);
    }

    @Override
    public Void createBucket(Bucket bucket) {
        return exceptionHandler.handle(() -> {
            try {
                client.createBlobContainer(bucket.getName());
            } catch (BlobStorageException error) {
                if (!exceptionHandler.isBucketAlreadyExists(error)) {
                    throw error;
                }
            }
            return null;
        });
    }

    @Override
    public Void deleteBucketIfExists(String bucketName) {
        return exceptionHandler.handle(() -> {
            client.getBlobContainerClient(bucketName).deleteIfExists();
            return null;
        });
    }

    @Override
    public byte[] getByteRange(String bucketName, String blobKey, long startInclusive, long endInclusive) {
        long length = ByteArrayRangeValidator.validateAndGetLength(startInclusive, endInclusive);
        return exceptionHandler.handle(() -> {
            BlobRange blobRange = new BlobRange(startInclusive, length);
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            blobClient(bucketName, blobKey)
                    .downloadStreamWithResponse(output, blobRange, null, null, false, null, null);
            return output.toByteArray();
        });
    }

    @Override
    public URL generateGetUrl(String bucket, String objectKey, Duration expiry) {
        validateExpiry(expiry);
        return exceptionHandler.handle(() -> {
            var blobClient = client.getBlobContainerClient(bucket).getBlobClient(objectKey);
            BlobSasPermission permission = new BlobSasPermission().setReadPermission(true);
            BlobServiceSasSignatureValues values = new BlobServiceSasSignatureValues(OffsetDateTime.now().plus(expiry), permission);
            String sas = blobClient.generateSas(values);
            return buildSasUrl(blobClient.getBlobUrl(), sas);
        });
    }

    @Override
    public URL generatePutUrl(String bucket, String objectKey, Duration expiry, String contentType) {
        validateExpiry(expiry);
        return exceptionHandler.handle(() -> {
            var blobClient = client.getBlobContainerClient(bucket).getBlobClient(objectKey);
            BlobSasPermission permission = new BlobSasPermission()
                    .setCreatePermission(true)
                    .setWritePermission(true);
            BlobServiceSasSignatureValues values = new BlobServiceSasSignatureValues(OffsetDateTime.now().plus(expiry), permission);
            String sas = blobClient.generateSas(values);
            return buildSasUrl(blobClient.getBlobUrl(), sas);
        });
    }

    private BlobClient blobClient(String bucketName, String blobKey) {
        return client.getBlobContainerClient(bucketName).getBlobClient(blobKey);
    }

    private List<Bucket> mapBuckets(Iterable<BlobContainerItem> containerItems) {
        List<Bucket> buckets = new ArrayList<>();
        containerItems.forEach(item -> {
            OffsetDateTime lastModified = item.getProperties() == null ? null : item.getProperties().getLastModified();
            buckets.add(Bucket.builder()
                    .name(item.getName())
                    .publicURI(toUri(client.getBlobContainerClient(item.getName()).getBlobContainerUrl()))
                    .lastModified(toLocalDateTime(lastModified))
                    .creationDate(null) // Not supported by azure
                    .build());
        });
        return buckets;
    }

    private List<Blob> mapBlobsFromList(String bucketName, BlobContainerClient containerClient, Iterable<BlobItem> blobItems) {
        List<Blob> blobs = new ArrayList<>();
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
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Failed to build SAS URL for Azure Blob Storage.", e);
        }
    }

    private void validateExpiry(Duration expiry) {
        if (expiry == null || expiry.isZero() || expiry.isNegative()) {
            throw new IllegalArgumentException("Expiry must be a positive duration.");
        }
    }

    private PageRequest normalizePageRequest(PageRequest request) {
        return request == null ? PageRequest.firstPage() : request;
    }

}
