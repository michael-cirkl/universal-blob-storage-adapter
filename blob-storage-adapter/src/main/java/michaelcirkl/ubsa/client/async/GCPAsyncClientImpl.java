package michaelcirkl.ubsa.client.async;


import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.BlobWriteSession;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.HttpMethod;
import com.google.cloud.storage.RangeSpec;
import com.google.cloud.storage.ReadAsFutureBytes;
import com.google.cloud.storage.ReadProjectionConfigs;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BucketListOption;
import com.google.cloud.storage.Storage.CopyRequest;
import com.google.cloud.storage.StorageException;
import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.BlobStorageAsyncClient;
import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.Provider;
import michaelcirkl.ubsa.UbsaException;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

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
    public <T> T unwrap(Class<T> nativeType) {
        if (nativeType == null) {
            throw new IllegalArgumentException("Class type to unwrap must not be null.");
        }
        return nativeType.isInstance(client) ? nativeType.cast(client) : null;
    }

    @Override
    public CompletableFuture<Boolean> bucketExists(String bucketName) {
        return wrapStorageException(
                CompletableFuture.supplyAsync(() -> client.get(bucketName) != null),
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
                                    .lastModified(toLocalDateTime(blobInfo.getUpdateTime()))
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
    public CompletableFuture<Void> deleteBucket(String bucketName) {
        return wrapStorageException(
                CompletableFuture.runAsync(() -> client.delete(bucketName)),
                "Failed to delete GCP bucket: " + bucketName
        );
    }

    @Override
    public CompletableFuture<Boolean> blobExists(String bucketName, String blobKey) {
        return wrapStorageException(
                CompletableFuture.supplyAsync(() -> client.get(bucketName, blobKey) != null),
                "Failed to check whether GCP blob exists: gs://" + bucketName + "/" + blobKey
        );
    }

    @Override
    public CompletableFuture<String> createBlob(String bucketName, Blob blob) {
        BlobInfo blobInfo = buildBlobInfo(bucketName, blob);
        byte[] content = blob.getContent() == null ? new byte[0] : blob.getContent();
        return wrapStorageException(
                CompletableFuture.supplyAsync(() -> writeBlobAsync(blobInfo, content))
                        .thenCompose(apiFuture -> toCompletableFuture(apiFuture))
                        .thenApply(BlobInfo::getEtag),
                "Failed to create GCP blob gs://" + bucketName + "/" + blob.getKey()
        );
    }

    @Override
    public CompletableFuture<Void> deleteBlob(String bucketName, String blobKey) {
        return wrapStorageException(
                CompletableFuture.runAsync(() -> client.delete(bucketName, blobKey)),
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
                }),
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
                        LocalDateTime created = toLocalDateTime(gcsBucket.getCreateTime());
                        buckets.add(Bucket.builder()
                                .name(gcsBucket.getName())
                                .publicURI(toGsUri(gcsBucket.getName(), null))
                                .creationDate(created)
                                .lastModified(created)
                                .build());
                    });
                    return buckets;
                }),
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
                }),
                "Failed to list GCP blobs in bucket " + bucketName
        );
    }

    @Override
    public CompletableFuture<Void> createBucket(Bucket bucket) {
        return wrapStorageException(
                CompletableFuture.runAsync(() -> client.create(BucketInfo.of(bucket.getName()))),
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
                CompletableFuture.runAsync(() -> client.delete(bucketName)),
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
    public CompletableFuture<String> createBlobIfNotExists(String bucketName, Blob blob) {
        return wrapStorageException(
                CompletableFuture.supplyAsync(() -> client.get(bucketName, blob.getKey()))
                        .thenCompose(existing -> {
                            if (existing != null) {
                                return CompletableFuture.completedFuture(existing.getEtag());
                            }
                            return createBlob(bucketName, blob);
                        }),
                "Failed to create GCP blob if not exists: gs://" + bucketName + "/" + blob.getKey()
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
            throw new UbsaException("Failed to generate GCP GET URL for gs://" + bucket + "/" + objectKey, error);
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
            throw new UbsaException("Failed to generate GCP PUT URL for gs://" + bucket + "/" + objectKey, error);
        }
    }

    private ApiFuture<BlobInfo> writeBlobAsync(BlobInfo blobInfo, byte[] content) {
        BlobWriteSession writeSession = client.blobWriteSession(blobInfo);
        try (WritableByteChannel channel = writeSession.open()) {
            ByteBuffer buffer = ByteBuffer.wrap(content);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
        } catch (IOException e) {
            throw new CompletionException("Failed to write blob content to GCS.", e);
        }
        return writeSession.getResult();
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

    private static <T> CompletableFuture<T> toCompletableFuture(ApiFuture<T> apiFuture) {
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

    private static BlobInfo buildBlobInfo(String bucketName, Blob blob) {
        BlobInfo.Builder blobBuilder = BlobInfo.newBuilder(bucketName, blob.getKey());
        if (blob.encoding() != null && !blob.encoding().isBlank()) {
            blobBuilder.setContentEncoding(blob.encoding());
        }
        Map<String, String> metadata = blob.getUserMetadata();
        if (metadata != null && !metadata.isEmpty()) {
            blobBuilder.setMetadata(metadata);
        }
        return blobBuilder.build();
    }

    private static Set<Blob> mapBlobsFromPage(String bucketName, Page<com.google.cloud.storage.Blob> blobPage) {
        Set<Blob> blobs = new HashSet<>();
        blobPage.iterateAll().forEach(gcsBlob -> blobs.add(Blob.builder()
                .bucket(bucketName)
                .key(gcsBlob.getName())
                .size(gcsBlob.getSize())
                .lastModified(toLocalDateTime(gcsBlob.getUpdateTime()))
                .encoding(gcsBlob.getContentEncoding())
                .etag(gcsBlob.getEtag())
                .userMetadata(gcsBlob.getMetadata())
                .publicURI(toGsUri(bucketName, gcsBlob.getName()))
                .build()));
        return blobs;
    }

    private static LocalDateTime toLocalDateTime(Long epochMilli) {
        if (epochMilli == null) {
            return null;
        }
        return Instant.ofEpochMilli(epochMilli).atOffset(ZoneOffset.UTC).toLocalDateTime();
    }

    private static URI toGsUri(String bucketName, String objectKey) {
        String uri = (objectKey == null || objectKey.isBlank())
                ? "gs://" + bucketName
                : "gs://" + bucketName + "/" + objectKey;
        return URI.create(uri);
    }

    private static long toPositiveSeconds(Duration expiry) {
        if (expiry == null || expiry.isZero() || expiry.isNegative()) {
            throw new IllegalArgumentException("Expiry must be a positive duration.");
        }
        return expiry.toSeconds();
    }

    private static void validateRange(long startInclusive, long endInclusive) {
        if (startInclusive < 0 || endInclusive < startInclusive) {
            throw new IllegalArgumentException("Invalid range. startInclusive must be >= 0 and endInclusive must be >= startInclusive.");
        }
    }

    private static void closeQuietly(BlobReadSession session) {
        try {
            session.close();
        } catch (IOException ignored) {
            // no-op
        }
    }

    private static <T> CompletableFuture<T> wrapStorageException(CompletableFuture<T> future, String message) {
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
        if (cause instanceof StorageException storageException) {
            return new CompletionException(new UbsaException(message, storageException));
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
