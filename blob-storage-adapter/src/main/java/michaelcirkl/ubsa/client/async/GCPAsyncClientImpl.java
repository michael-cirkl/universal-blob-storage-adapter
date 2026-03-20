package michaelcirkl.ubsa.client.async;


import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.*;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BucketListOption;
import com.google.cloud.storage.Storage.CopyRequest;
import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.*;
import michaelcirkl.ubsa.client.streaming.*;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.channels.WritableByteChannel;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Function;

public class GCPAsyncClientImpl implements BlobStorageAsyncClient {
    private static final ExecutorService IO_EXECUTOR = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "ubsa-gcp-async-io");
        thread.setDaemon(true);
        return thread;
    });

    private final Storage client;

    public GCPAsyncClientImpl(Storage client) {
        this.client = client;
    }

    @Override
    public Provider getProvider() {
        return Provider.GCP;
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
        return wrapStorageException(
                CompletableFuture.supplyAsync(() -> client.get(bucketName) != null, IO_EXECUTOR),
                "Failed to check whether GCP bucket exists: " + bucketName
        );
    }

    @Override
    public CompletableFuture<Blob> getBlob(String bucketName, String blobKey) {
        BlobId blobId = BlobId.of(bucketName, blobKey);
        return wrapStorageException(
                withBlobReadSession(blobId, session -> {
                    BlobInfo blobInfo = session.getBlobInfo();
                    return toCompletableFuture(session.readAs(ReadProjectionConfigs.asFutureBytes()))
                            .thenApply(content -> Blob.builder()
                                    .bucket(bucketName)
                                    .key(blobKey)
                                    .content(content)
                                    .size(blobInfo.getSize() == null ? 0L : blobInfo.getSize())
                                    .lastModified(toLocalDateTime(blobInfo.getUpdateTimeOffsetDateTime()))
                                    .encoding(blobInfo.getContentEncoding())
                                    .etag(blobInfo.getEtag())
                                    .userMetadata(blobInfo.getMetadata())
                                    .publicURI(toGsUri(bucketName, blobKey))
                                    .build());
                }),
                "Failed to get GCP blob gs://" + bucketName + "/" + blobKey
        );
    }

    @Override
    public CompletableFuture<Flow.Publisher<ByteBuffer>> openBlobStream(String bucketName, String blobKey) {
        return wrapStorageException(
                CompletableFuture.completedFuture(new GcpReadChannelFlowPublisher(client, BlobId.of(bucketName, blobKey), IO_EXECUTOR)),
                "Failed to open GCP blob stream gs://" + bucketName + "/" + blobKey
        );
    }

    @Override
    public CompletableFuture<Void> deleteBucket(String bucketName) {
        return wrapStorageException(
                CompletableFuture.runAsync(() -> client.delete(bucketName), IO_EXECUTOR),
                "Failed to delete GCP bucket: " + bucketName
        );
    }

    @Override
    public CompletableFuture<Boolean> blobExists(String bucketName, String blobKey) {
        return wrapStorageException(
                CompletableFuture.supplyAsync(() -> client.get(bucketName, blobKey) != null, IO_EXECUTOR),
                "Failed to check whether GCP blob exists: gs://" + bucketName + "/" + blobKey
        );
    }

    @Override
    public CompletableFuture<String> createBlob(String bucketName, Blob blob) {
        BlobInfo blobInfo = buildBlobInfo(bucketName, blob);
        byte[] content = blob.getContent() == null ? new byte[0] : blob.getContent();
        return wrapStorageException(
                CompletableFuture.supplyAsync(() -> writeBlobAsync(blobInfo, content), IO_EXECUTOR)
                        .thenCompose(this::toCompletableFuture)
                        .thenApply(BlobInfo::getEtag),
                "Failed to create GCP blob gs://" + bucketName + "/" + blob.getKey()
        );
    }

    @Override
    public CompletableFuture<String> createBlob(String bucketName, String blobKey, Path sourceFile) {
        FileUploadValidators.validateSourceFile(sourceFile);
        BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, blobKey).build();
        return wrapStorageException(
                CompletableFuture.supplyAsync(() -> createBlobFromFile(blobInfo, sourceFile), IO_EXECUTOR),
                "Failed to create GCP blob gs://" + bucketName + "/" + blobKey + " from file"
        );
    }

    @Override
    public CompletableFuture<String> createBlob(String bucketName, String blobKey, Flow.Publisher<ByteBuffer> content, long contentLength, BlobWriteOptions options) {
        ContentLengthValidators.validateContentLength(contentLength);
        if (content == null) {
            throw new IllegalArgumentException("Content publisher must not be null.");
        }
        BlobInfo blobInfo = buildBlobInfo(bucketName, blobKey, options);
        return wrapStorageException(
                CompletableFuture.supplyAsync(() -> writeBlobAsync(blobInfo, content, contentLength), IO_EXECUTOR)
                        .thenCompose(this::toCompletableFuture)
                        .thenApply(BlobInfo::getEtag),
                "Failed to stream-create GCP blob gs://" + bucketName + "/" + blobKey
        );
    }

    @Override
    public CompletableFuture<Void> deleteBlob(String bucketName, String blobKey) {
        return wrapStorageException(
                CompletableFuture.runAsync(() -> client.delete(bucketName, blobKey), IO_EXECUTOR),
                "Failed to delete GCP blob gs://" + bucketName + "/" + blobKey
        );
    }

    @Override
    public CompletableFuture<String> copyBlob(String sourceBucketName, String sourceBlobKey, String destinationBucketName, String destinationBlobKey) {
        return wrapStorageException(
                CompletableFuture.supplyAsync(() -> {
                    CopyRequest request = CopyRequest.newBuilder()
                            .setSource(BlobId.of(sourceBucketName, sourceBlobKey))
                            .setTarget(BlobId.of(destinationBucketName, destinationBlobKey))
                            .build();
                    return client.copy(request).getResult().getEtag();
                }, IO_EXECUTOR),
                "Failed to copy GCP blob from gs://" + sourceBucketName + "/" + sourceBlobKey
                        + " to gs://" + destinationBucketName + "/" + destinationBlobKey
        );
    }

    @Override
    public CompletableFuture<Set<Bucket>> listAllBuckets() {
        return wrapStorageException(
                CompletableFuture.supplyAsync(() -> {
                    Set<Bucket> buckets = new HashSet<>();
                    Page<com.google.cloud.storage.Bucket> bucketPage = client.list(BucketListOption.pageSize(1000));
                    bucketPage.iterateAll().forEach(gcsBucket -> {
                        LocalDateTime created = toLocalDateTime(gcsBucket.getCreateTimeOffsetDateTime());
                        buckets.add(Bucket.builder()
                                .name(gcsBucket.getName())
                                .publicURI(toGsUri(gcsBucket.getName(), null))
                                .creationDate(created)
                                .lastModified(created)
                                .build());
                    });
                    return buckets;
                }, IO_EXECUTOR),
                "Failed to list GCP buckets"
        );
    }

    @Override
    public CompletableFuture<Set<Blob>> listBlobsByPrefix(String bucketName, String prefix) {
        return wrapStorageException(
                CompletableFuture.supplyAsync(() -> {
                    Page<com.google.cloud.storage.Blob> blobPage = (prefix != null && !prefix.isBlank())
                            ? client.list(bucketName, BlobListOption.prefix(prefix))
                            : client.list(bucketName);
                    return mapBlobsFromPage(bucketName, blobPage);
                }, IO_EXECUTOR),
                "Failed to list GCP blobs in bucket " + bucketName
        );
    }

    @Override
    public CompletableFuture<Void> createBucket(Bucket bucket) {
        return wrapStorageException(
                CompletableFuture.runAsync(() -> client.create(BucketInfo.of(bucket.getName())), IO_EXECUTOR),
                "Failed to create GCP bucket " + bucket.getName()
        );
    }

    @Override
    public CompletableFuture<Set<Blob>> getAllBlobsInBucket(String bucketName) {
        return listBlobsByPrefix(bucketName, null);
    }

    @Override
    public CompletableFuture<Void> deleteBucketIfExists(String bucketName) {
        return wrapStorageException(
                CompletableFuture.runAsync(() -> client.delete(bucketName), IO_EXECUTOR),
                "Failed to delete GCP bucket if exists: " + bucketName
        );
    }

    @Override
    public CompletableFuture<byte[]> getByteRange(String bucketName, String blobKey, long startInclusive, long endInclusive) {
        validateRange(startInclusive, endInclusive);
        long length = endInclusive - startInclusive + 1;
        BlobId blobId = BlobId.of(bucketName, blobKey);
        return wrapStorageException(
                withBlobReadSession(blobId, session -> {
                    ReadAsFutureBytes readConfig = ReadProjectionConfigs.asFutureBytes()
                            .withRangeSpec(RangeSpec.beginAt(startInclusive).withMaxLength(length));
                    return toCompletableFuture(session.readAs(readConfig));
                }),
                "Failed to read byte range from GCP blob gs://" + bucketName + "/" + blobKey
        );
    }

    @Override
    public CompletableFuture<URL> generateGetUrl(String bucket, String objectKey, Duration expiry) {
        try {
            long seconds = toPositiveSeconds(expiry);
            BlobInfo blobInfo = BlobInfo.newBuilder(bucket, objectKey).build();
            URL url = client.signUrl(blobInfo, seconds, TimeUnit.SECONDS);
            return CompletableFuture.completedFuture(url);
        } catch (StorageException error) {
            return CompletableFuture.failedFuture(
                    new UbsaException("Failed to generate GCP GET URL for gs://" + bucket + "/" + objectKey, error)
            );
        }
    }

    @Override
    public CompletableFuture<URL> generatePutUrl(String bucket, String objectKey, Duration expiry, String contentType) {
        try {
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
        } catch (StorageException error) {
            return CompletableFuture.failedFuture(
                    new UbsaException("Failed to generate GCP PUT URL for gs://" + bucket + "/" + objectKey, error)
            );
        }
    }

    private ApiFuture<BlobInfo> writeBlobAsync(BlobInfo blobInfo, byte[] content, Storage.BlobWriteOption... writeOptions) {
        BlobWriteSession writeSession = client.blobWriteSession(blobInfo, writeOptions);
        try (WritableByteChannel channel = writeSession.open()) {
            ByteBuffer buffer = ByteBuffer.wrap(content);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
        } catch (IOException e) {
            throw new UbsaException("Failed to write blob content to GCS.", e);
        }
        return writeSession.getResult();
    }

    private ApiFuture<BlobInfo> writeBlobAsync(BlobInfo blobInfo, byte[] content) {
        return writeBlobAsync(blobInfo, content, new Storage.BlobWriteOption[0]);
    }

    private ApiFuture<BlobInfo> writeBlobAsync(BlobInfo blobInfo, Flow.Publisher<ByteBuffer> content, long contentLength) {
        BlobWriteSession writeSession = client.blobWriteSession(blobInfo);
        try (WritableByteChannel channel = writeSession.open()) {
            GcpFlowPublisherChannelWriter.writeFromPublisher(content, channel, contentLength);
        } catch (IOException e) {
            throw new UbsaException("Failed to write streamed blob content to GCS.", e);
        }
        return writeSession.getResult();
    }

    private String createBlobFromFile(BlobInfo blobInfo, Path sourceFile) {
        try {
            return client.createFrom(blobInfo, sourceFile).getEtag();
        } catch (IOException e) {
            throw new UbsaException(
                    "Failed to create GCP blob gs://" + blobInfo.getBucket() + "/" + blobInfo.getName() + " from file", e
            );
        }
    }

    private <T> CompletableFuture<T> withBlobReadSession(
            BlobId blobId,
            Function<BlobReadSession, CompletableFuture<T>> action
    ) {
        return toCompletableFuture(client.blobReadSession(blobId))
                .thenCompose(session -> {
                    CompletableFuture<T> actionFuture;
                    try {
                        actionFuture = action.apply(session);
                    } catch (Throwable error) {
                        closeQuietly(session);
                        return CompletableFuture.failedFuture(error);
                    }
                    return actionFuture.whenComplete((ignored, error) -> closeQuietly(session));
                });
    }

    private <T> CompletableFuture<T> toCompletableFuture(ApiFuture<T> apiFuture) {
        CompletableFuture<T> future = new CompletableFuture<>();
        ApiFutures.addCallback(apiFuture, new ApiFutureCallback<>() {
            @Override
            public void onFailure(Throwable throwable) {
                future.completeExceptionally(throwable);
            }

            @Override
            public void onSuccess(T result) {
                future.complete(result);
            }
        }, Runnable::run);
        return future;
    }

    private BlobInfo buildBlobInfo(String bucketName, Blob blob) {
        BlobInfo.Builder blobBuilder = BlobInfo.newBuilder(bucketName, blob.getKey());
        WriteOptionsMappers.applyBlobToGcpBlobInfo(blobBuilder, blob);
        return blobBuilder.build();
    }

    private BlobInfo buildBlobInfo(String bucketName, String blobKey, BlobWriteOptions options) {
        BlobInfo.Builder blobBuilder = BlobInfo.newBuilder(bucketName, blobKey);
        WriteOptionsMappers.applyOptionsToGcpBlobInfo(blobBuilder, options);
        return blobBuilder.build();
    }

    private Set<Blob> mapBlobsFromPage(String bucketName, Page<com.google.cloud.storage.Blob> blobPage) {
        Set<Blob> blobs = new HashSet<>();
        blobPage.iterateAll().forEach(gcsBlob -> blobs.add(Blob.builder()
                .bucket(bucketName)
                .key(gcsBlob.getName())
                .size(gcsBlob.getSize())
                .lastModified(toLocalDateTime(gcsBlob.getUpdateTimeOffsetDateTime()))
                .encoding(gcsBlob.getContentEncoding())
                .etag(gcsBlob.getEtag())
                .userMetadata(gcsBlob.getMetadata())
                .publicURI(toGsUri(bucketName, gcsBlob.getName()))
                .build()));
        return blobs;
    }

    private LocalDateTime toLocalDateTime(OffsetDateTime time) {
        return time == null ? null : time.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime();
    }

    private URI toGsUri(String bucketName, String objectKey) {
        String uri = (objectKey == null || objectKey.isBlank())
                ? "gs://" + bucketName
                : "gs://" + bucketName + "/" + objectKey;
        return URI.create(uri);
    }

    private long toPositiveSeconds(Duration expiry) {
        if (expiry == null || expiry.isZero() || expiry.isNegative()) {
            throw new IllegalArgumentException("Expiry must be a positive duration.");
        }
        return expiry.toSeconds();
    }

    private void validateRange(long startInclusive, long endInclusive) {
        if (startInclusive < 0 || endInclusive < startInclusive) {
            throw new IllegalArgumentException("Invalid range. startInclusive must be >= 0 and endInclusive must be >= startInclusive.");
        }
    }

    private void closeQuietly(BlobReadSession session) {
        try {
            session.close();
        } catch (IOException ignored) {

        }
    }

    private boolean isPreconditionFailed(Throwable error) {
        return error instanceof StorageException storageException
                && storageException.getCode() == 412;
    }

    private String getExistingBlobEtag(String bucketName, String blobKey) {
        com.google.cloud.storage.Blob existing = client.get(bucketName, blobKey);
        if (existing == null) {
            throw new IllegalStateException("Blob not found after conditional create attempt: gs://" + bucketName + "/" + blobKey);
        }
        return existing.getEtag();
    }

    private <T> CompletableFuture<T> wrapStorageException(CompletableFuture<T> future, String message) {
        return StreamErrorAdapters.wrapUbsaFuture(future, message, StorageException.class);
    }
}
