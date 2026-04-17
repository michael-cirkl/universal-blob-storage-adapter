package io.github.michaelcirkl.ubsa.client.azure;

import com.azure.core.http.rest.PagedResponse;
import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.models.*;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.blob.options.BlobUploadFromFileOptions;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import io.github.michaelcirkl.ubsa.Blob;
import io.github.michaelcirkl.ubsa.BlobStorageAsyncClient;
import io.github.michaelcirkl.ubsa.Bucket;
import io.github.michaelcirkl.ubsa.Provider;
import io.github.michaelcirkl.ubsa.client.exception.AzureExceptionHandler;
import io.github.michaelcirkl.ubsa.client.pagination.AsyncBucketListingSupport;
import io.github.michaelcirkl.ubsa.client.pagination.ListingPage;
import io.github.michaelcirkl.ubsa.client.pagination.PageRequest;
import io.github.michaelcirkl.ubsa.client.streaming.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

public class AzureAsyncClientImpl implements BlobStorageAsyncClient {
    private final AzureExceptionHandler exceptionHandler = new AzureExceptionHandler();
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
        return exceptionHandler.handleAsync(
                client.getBlobContainerAsyncClient(bucketName)
                        .exists()
                        .toFuture()
        );
    }

    @Override
    public CompletableFuture<Blob> getBlob(String bucketName, String blobKey) {
        BlobAsyncClient blobClient = blobClient(bucketName, blobKey);
        CompletableFuture<BlobProperties> propertiesFuture = blobClient.getProperties().toFuture();
        CompletableFuture<BinaryData> contentFuture = blobClient.downloadContent().toFuture();

        return exceptionHandler.handleAsync(
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
                        .build())
        );
    }

    @Override
    public CompletableFuture<Blob> getBlobMetadata(String bucketName, String blobKey) {
        BlobAsyncClient blobClient = blobClient(bucketName, blobKey);

        return exceptionHandler.handleAsync(
                blobClient.getProperties()
                        .map(properties -> Blob.builder()
                                .bucket(bucketName)
                                .key(blobKey)
                                .size(properties.getBlobSize())
                                .lastModified(toLocalDateTime(properties.getLastModified()))
                                .encoding(properties.getContentEncoding())
                                .etag(properties.getETag())
                                .userMetadata(properties.getMetadata())
                                .publicURI(toUri(blobClient.getBlobUrl()))
                                .expires(toLocalDateTime(properties.getExpiresOn()))
                                .build())
                        .toFuture()
        );
    }

    @Override
    public Flow.Publisher<ByteBuffer> openBlobStream(String bucketName, String blobKey) {
        BlobAsyncClient blobClient = blobClient(bucketName, blobKey);
        return FlowPublisherBridge.mapErrors(
                FlowPublisherBridge.toFlowPublisher(blobClient.downloadStream()),
                exceptionHandler::propagate
        );
    }

    @Override
    public CompletableFuture<Void> deleteBucket(String bucketName) {
        return exceptionHandler.handleAsync(
                client.getBlobContainerAsyncClient(bucketName)
                        .delete()
                        .toFuture()
        );
    }

    @Override
    public CompletableFuture<Boolean> blobExists(String bucketName, String blobKey) {
        return exceptionHandler.handleAsync(
                blobClient(bucketName, blobKey)
                        .exists()
                        .toFuture()
        );
    }

    @Override
    public CompletableFuture<String> createBlob(String bucketName, Blob blob) {
        BlobAsyncClient blobClient = blobClient(bucketName, blob.getKey());
        BlobParallelUploadOptions uploadOptions = WriteOptionsMappers.buildAzureUploadOptions(blob);
        return exceptionHandler.handleAsync(
                blobClient.uploadWithResponse(uploadOptions)
                        .map(response -> response.getValue().getETag())
                        .toFuture()
        );
    }

    @Override
    public CompletableFuture<String> createBlob(String bucketName, String blobKey, Path sourceFile) {
        return createBlob(bucketName, blobKey, sourceFile, null);
    }

    @Override
    public CompletableFuture<String> createBlob(String bucketName, String blobKey, Path sourceFile, BlobWriteOptions options) {
        FileUploadValidators.validateSourceFile(sourceFile);
        BlobAsyncClient blobClient = blobClient(bucketName, blobKey);
        BlobHttpHeaders headers = WriteOptionsMappers.toAzureHeaders(options);
        Map<String, String> metadata = WriteOptionsMappers.toAzureMetadata(options);
        BlobUploadFromFileOptions uploadOptions = new BlobUploadFromFileOptions(sourceFile.toString())
                .setHeaders(headers)
                .setMetadata(metadata);
        return exceptionHandler.handleAsync(
                blobClient.uploadFromFileWithResponse(uploadOptions)
                        .map(response -> response.getValue().getETag())
                        .toFuture()
        );
    }

    @Override
    public CompletableFuture<String> createBlob(String bucketName, String blobKey, Flow.Publisher<ByteBuffer> content, long contentLength, BlobWriteOptions options) {
        ContentLengthValidators.validateContentLength(contentLength);
        if (content == null) {
            throw new IllegalArgumentException("Content publisher must not be null.");
        }
        BlobAsyncClient blobClient = blobClient(bucketName, blobKey);
        Flux<ByteBuffer> flux = Flux.from(FlowPublisherBridge.toReactivePublisher(content));
        BlobHttpHeaders headers = WriteOptionsMappers.toAzureHeaders(options);
        Map<String, String> metadata = WriteOptionsMappers.toAzureMetadata(options);
        return exceptionHandler.handleAsync(
                blobClient.getBlockBlobAsyncClient()
                        .uploadWithResponse(flux, contentLength, headers, metadata, null, null, null)
                        .map(response -> response.getValue().getETag())
                        .toFuture()
        );
    }

    @Override
    public CompletableFuture<Void> deleteBlobIfExists(String bucketName, String blobKey) {
        return exceptionHandler.handleAsync(
                blobClient(bucketName, blobKey)
                        .deleteIfExists()
                        .then()
                        .toFuture()
        );
    }

    @Override
    public CompletableFuture<String> copyBlob(String sourceBucketName, String sourceBlobKey, String destinationBucketName, String destinationBlobKey) {
        BlobAsyncClient sourceBlobClient = blobClient(sourceBucketName, sourceBlobKey);
        BlobAsyncClient destinationBlobClient = blobClient(destinationBucketName, destinationBlobKey);
        return exceptionHandler.handleAsync(
                destinationBlobClient.copyFromUrl(sourceBlobClient.getBlobUrl())
                        .flatMap(copyId -> destinationBlobClient.getProperties().map(BlobProperties::getETag))
                        .toFuture()
        );
    }

    @Override
    public CompletableFuture<ListingPage<Bucket>> listBuckets(PageRequest request) {
        PageRequest pageRequest = normalizePageRequest(request);
        String continuationToken = pageRequest.getContinuationToken();
        Integer pageSize = pageRequest.getPageSize();
        Mono<PagedResponse<BlobContainerItem>> pageMono;
        if (continuationToken == null) {
            pageMono = pageSize == null
                    ? client.listBlobContainers().byPage().next()
                    : client.listBlobContainers().byPage(pageSize).next();
        } else {
            pageMono = pageSize == null
                    ? client.listBlobContainers().byPage(continuationToken).next()
                    : client.listBlobContainers().byPage(continuationToken, pageSize).next();
        }
        return exceptionHandler.handleAsync(
                pageMono
                        .map(page -> ListingPage.of(mapBuckets(page.getElements()), page.getContinuationToken()))
                        .defaultIfEmpty(ListingPage.of(List.of(), null))
                        .toFuture()
        );
    }

    @Override
    public CompletableFuture<ListingPage<Blob>> listBlobs(String bucketName, String prefix, PageRequest request) {
        PageRequest pageRequest = normalizePageRequest(request);
        BlobContainerAsyncClient containerClient = client.getBlobContainerAsyncClient(bucketName);
        ListBlobsOptions options = new ListBlobsOptions();
        options.setDetails(new BlobListDetails().setRetrieveMetadata(true));
        if (prefix != null && !prefix.isBlank()) {
            options.setPrefix(prefix);
        }
        String continuationToken = pageRequest.getContinuationToken();
        Integer pageSize = pageRequest.getPageSize();
        Mono<PagedResponse<BlobItem>> pageMono;
        if (continuationToken == null) {
            pageMono = pageSize == null
                    ? containerClient.listBlobs(options, null).byPage().next()
                    : containerClient.listBlobs(options, null).byPage(pageSize).next();
        } else {
            pageMono = pageSize == null
                    ? containerClient.listBlobs(options, null).byPage(continuationToken).next()
                    : containerClient.listBlobs(options, null).byPage(continuationToken, pageSize).next();
        }

        return exceptionHandler.handleAsync(
                pageMono
                        .map(page -> ListingPage.of(
                                mapBlobsFromList(bucketName, containerClient, page.getElements()),
                                page.getContinuationToken()
                        ))
                        .defaultIfEmpty(ListingPage.of(List.of(), null))
                        .toFuture()
        );
    }

    @Override
    public CompletableFuture<List<Bucket>> listAllBuckets() {
        return AsyncBucketListingSupport.listAllBuckets(this::listBuckets);
    }

    @Override
    public CompletableFuture<Void> createBucket(Bucket bucket) {
        return client.createBlobContainer(bucket.getName())
                .then()
                .toFuture()
                .handle((result, error) -> {
                    if (error == null) {
                        return null;
                    }

                    Throwable cause = exceptionHandler.unwrap(error);
                    if (cause instanceof BlobStorageException blobStorageException
                            && exceptionHandler.isBucketAlreadyExists(blobStorageException)) {
                        return null;
                    }
                    throw exceptionHandler.propagate(cause);
                });
    }

    @Override
    public CompletableFuture<Void> deleteBucketIfExists(String bucketName) {
        return exceptionHandler.handleAsync(
                client.getBlobContainerAsyncClient(bucketName)
                        .deleteIfExists()
                        .then()
                        .toFuture()
        );
    }

    @Override
    public CompletableFuture<byte[]> getByteRange(String bucketName, String blobKey, long startInclusive, long endInclusive) {
        long length = ByteArrayRangeValidator.validateAndGetLength(startInclusive, endInclusive);
        BlobRange blobRange = new BlobRange(startInclusive, length);

        return exceptionHandler.handleAsync(
                blobClient(bucketName, blobKey)
                        .downloadStreamWithResponse(blobRange, null, null, false)
                        .flatMap(response -> BinaryData.fromFlux(response.getValue()))
                        .map(BinaryData::toBytes)
                        .toFuture()
        );
    }



    @Override
    public URL generateGetUrl(String bucket, String objectKey, Duration expiry) {
        validateExpiry(expiry);
        return exceptionHandler.handle(() -> {
            var blobClient = client.getBlobContainerAsyncClient(bucket).getBlobAsyncClient(objectKey);
            BlobSasPermission permission = new BlobSasPermission()
                    .setReadPermission(true);
            BlobServiceSasSignatureValues values = new BlobServiceSasSignatureValues(OffsetDateTime.now().plus(expiry), permission);
            String sas = blobClient.generateSas(values);
            return buildSasUrl(blobClient.getBlobUrl(), sas);
        });
    }

    @Override
    public URL generatePutUrl(String bucket, String objectKey, Duration expiry) {
        validateExpiry(expiry);
        return exceptionHandler.handle(() -> {
            var blobClient = client.getBlobContainerAsyncClient(bucket).getBlobAsyncClient(objectKey);
            BlobSasPermission permission = new BlobSasPermission()
                    .setCreatePermission(true)
                    .setWritePermission(true);
            BlobServiceSasSignatureValues values = new BlobServiceSasSignatureValues(OffsetDateTime.now().plus(expiry), permission);
            String sas = blobClient.generateSas(values);
            return buildSasUrl(blobClient.getBlobUrl(), sas);
        });
    }

    private BlobAsyncClient blobClient(String bucketName, String blobKey) {
        return client.getBlobContainerAsyncClient(bucketName).getBlobAsyncClient(blobKey);
    }

    private List<Bucket> mapBuckets(Iterable<BlobContainerItem> containerItems) {
        List<Bucket> buckets = new ArrayList<>();
        containerItems.forEach(item -> {
            OffsetDateTime lastModified = item.getProperties() == null
                    ? null
                    : item.getProperties().getLastModified();
            buckets.add(Bucket.builder()
                    .name(item.getName())
                    .publicURI(toUri(client.getBlobContainerAsyncClient(item.getName()).getBlobContainerUrl()))
                    .creationDate(null) // Not supported by azure
                    .lastModified(toLocalDateTime(lastModified))
                    .build());
        });
        return buckets;
    }

    private List<Blob> mapBlobsFromList(String bucketName, BlobContainerAsyncClient containerClient, Iterable<BlobItem> blobItems) {
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
                    .publicURI(toUri(containerClient.getBlobAsyncClient(item.getName()).getBlobUrl()))
                    .expires(toLocalDateTime(properties == null ? null : properties.getExpiryTime()))
                    .build());
        });
        return blobs;
    }

    private LocalDateTime toLocalDateTime(OffsetDateTime time) {
        return time == null ? null : time.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime();
    }

    private URI toUri(String uriValue) {
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
