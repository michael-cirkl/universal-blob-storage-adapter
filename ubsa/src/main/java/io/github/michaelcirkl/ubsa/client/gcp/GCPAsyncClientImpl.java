package io.github.michaelcirkl.ubsa.client.gcp;


import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.*;
import com.google.cloud.storage.Storage.CopyRequest;
import io.github.michaelcirkl.ubsa.BlobStorageAsyncClient;
import io.github.michaelcirkl.ubsa.Provider;
import io.github.michaelcirkl.ubsa.client.exception.GCPExceptionHandler;
import io.github.michaelcirkl.ubsa.client.pagination.AsyncBucketListingSupport;
import io.github.michaelcirkl.ubsa.client.pagination.ListingPage;
import io.github.michaelcirkl.ubsa.client.pagination.PageRequest;
import io.github.michaelcirkl.ubsa.client.streaming.*;

import java.io.ByteArrayOutputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;

public class GCPAsyncClientImpl implements BlobStorageAsyncClient {
    private final GCPExceptionHandler exceptionHandler = new GCPExceptionHandler();
    private final Storage client;
    private static final ExecutorService IO_EXECUTOR = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "ubsa-gcp-async-io");
        thread.setDaemon(true);
        return thread;
    });

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
        return exceptionHandler.handleAsync(
                CompletableFuture.supplyAsync(() -> client.get(bucketName) != null, IO_EXECUTOR)
        );
    }

    @Override
    public CompletableFuture<io.github.michaelcirkl.ubsa.Blob> getBlob(String bucketName, String blobKey) {
        return exceptionHandler.handleAsync(
                CompletableFuture.supplyAsync(() -> {
                    com.google.cloud.storage.Blob blob = requireBlob(bucketName, blobKey);
                    byte[] content = client.readAllBytes(
                            BlobId.of(bucketName, blobKey),
                            Storage.BlobSourceOption.shouldReturnRawInputStream(true)
                    );
                    return GCPClientSupport.mapFetchedBlob(bucketName, blobKey, blob, content);
                }, IO_EXECUTOR)
        );
    }

    @Override
    public CompletableFuture<io.github.michaelcirkl.ubsa.Blob> getBlobMetadata(String bucketName, String blobKey) {
        return exceptionHandler.handleAsync(
                CompletableFuture.supplyAsync(() -> {
                    com.google.cloud.storage.Blob blob = client.get(bucketName, blobKey);
                    if (blob == null) {
                        throw new StorageException(404, "Blob not found: gs://" + bucketName + "/" + blobKey);
                    }
                    return GCPClientSupport.mapBlobMetadata(bucketName, blobKey, blob);
                }, IO_EXECUTOR)
        );
    }

    @Override
    public Flow.Publisher<ByteBuffer> openBlobStream(String bucketName, String blobKey) {
        return FlowPublisherBridge.mapErrors(
                new GCPReadChannelFlowPublisher(client, BlobId.of(bucketName, blobKey), IO_EXECUTOR),
                exceptionHandler::propagate
        );
    }

    @Override
    public CompletableFuture<Void> deleteBucket(String bucketName) {
        return exceptionHandler.handleAsync(
                CompletableFuture.supplyAsync(() -> {
                    if (!client.delete(bucketName)) {
                        throw new StorageException(404, "Bucket not found: " + bucketName);
                    }
                    return null;
                }, IO_EXECUTOR)
        );
    }

    @Override
    public CompletableFuture<Boolean> blobExists(String bucketName, String blobKey) {
        return exceptionHandler.handleAsync(
                CompletableFuture.supplyAsync(() -> client.get(bucketName, blobKey) != null, IO_EXECUTOR)
        );
    }

    @Override
    public CompletableFuture<String> createBlob(String bucketName, io.github.michaelcirkl.ubsa.Blob blob) {
        BlobInfo blobInfo = buildBlobInfo(bucketName, blob);
        byte[] content = blob.getContent() == null ? new byte[0] : blob.getContent();
        return exceptionHandler.handleAsync(
                CompletableFuture.supplyAsync(() -> writeBlobAsync(blobInfo, content), IO_EXECUTOR)
                        .thenCompose(this::toCompletableFuture)
                        .thenApply(BlobInfo::getEtag)
        );
    }

    @Override
    public CompletableFuture<String> createBlob(String bucketName, String blobKey, Path sourceFile) {
        return createBlob(bucketName, blobKey, sourceFile, null);
    }

    @Override
    public CompletableFuture<String> createBlob(String bucketName, String blobKey, Path sourceFile, BlobWriteOptions options) {
        FileUploadValidators.validateSourceFile(sourceFile);
        BlobInfo blobInfo = buildBlobInfo(bucketName, blobKey, options);
        return exceptionHandler.handleAsync(
                CompletableFuture.supplyAsync(() -> createBlobFromFile(blobInfo, sourceFile), IO_EXECUTOR)
        );
    }

    @Override
    public CompletableFuture<String> createBlob(String bucketName, String blobKey, Flow.Publisher<ByteBuffer> content, long contentLength, BlobWriteOptions options) {
        ContentLengthValidators.validateContentLength(contentLength);
        if (content == null) {
            throw new IllegalArgumentException("Content publisher must not be null.");
        }
        BlobInfo blobInfo = buildBlobInfo(bucketName, blobKey, options);
        return exceptionHandler.handleAsync(
                CompletableFuture.supplyAsync(() -> writeBlobAsync(blobInfo, content, contentLength), IO_EXECUTOR)
                        .thenCompose(this::toCompletableFuture)
                        .thenApply(BlobInfo::getEtag)
        );
    }

    @Override
    public CompletableFuture<Void> deleteBlobIfExists(String bucketName, String blobKey) {
        return exceptionHandler.handleAsync(
                CompletableFuture.runAsync(() -> client.delete(bucketName, blobKey), IO_EXECUTOR)
        );
    }

    @Override
    public CompletableFuture<String> copyBlob(String sourceBucketName, String sourceBlobKey, String destinationBucketName, String destinationBlobKey) {
        return exceptionHandler.handleAsync(
                CompletableFuture.supplyAsync(() -> {
                    CopyRequest request = CopyRequest.newBuilder()
                            .setSource(BlobId.of(sourceBucketName, sourceBlobKey))
                            .setTarget(BlobId.of(destinationBucketName, destinationBlobKey))
                            .build();
                    return client.copy(request).getResult().getEtag();
                }, IO_EXECUTOR)
        );
    }

    @Override
    public CompletableFuture<ListingPage<io.github.michaelcirkl.ubsa.Bucket>> listBuckets(PageRequest request) {
        PageRequest pageRequest = GCPClientSupport.normalizePageRequest(request);
        return exceptionHandler.handleAsync(
                CompletableFuture.supplyAsync(() -> {
                    Page<com.google.cloud.storage.Bucket> bucketPage = client.list(GCPClientSupport.buildBucketListOptions(pageRequest));
                    return ListingPage.of(GCPClientSupport.mapBuckets(bucketPage.getValues()), bucketPage.getNextPageToken());
                }, IO_EXECUTOR)
        );
    }

    @Override
    public CompletableFuture<ListingPage<io.github.michaelcirkl.ubsa.Blob>> listBlobs(String bucketName, String prefix, PageRequest request) {
        PageRequest pageRequest = GCPClientSupport.normalizePageRequest(request);
        return exceptionHandler.handleAsync(
                CompletableFuture.supplyAsync(() -> {
                    Page<com.google.cloud.storage.Blob> blobPage = client.list(bucketName, GCPClientSupport.buildBlobListOptions(prefix, pageRequest));
                    return ListingPage.of(GCPClientSupport.mapBlobsFromPage(bucketName, blobPage.getValues()), blobPage.getNextPageToken());
                }, IO_EXECUTOR)
        );
    }

    @Override
    public CompletableFuture<List<io.github.michaelcirkl.ubsa.Bucket>> listAllBuckets() {
        return AsyncBucketListingSupport.listAllBuckets(this::listBuckets);
    }

    @Override
    public CompletableFuture<Void> createBucket(io.github.michaelcirkl.ubsa.Bucket bucket) {
        return exceptionHandler.handleAsync(
                CompletableFuture.runAsync(() -> {
                    try {
                        client.create(BucketInfo.of(bucket.getName()));
                    } catch (StorageException error) {
                        if (!exceptionHandler.isBucketAlreadyExists(error)) {
                            throw error;
                        }
                    }
                }, IO_EXECUTOR)
        );
    }

    @Override
    public CompletableFuture<Void> deleteBucketIfExists(String bucketName) {
        return CompletableFuture.runAsync(() -> {
            try {
                client.delete(bucketName);
            } catch (StorageException error) {
                if (!exceptionHandler.isNotFound(error)) {
                    throw exceptionHandler.wrap(error);
                }
            }
        }, IO_EXECUTOR);
    }

    @Override
    public CompletableFuture<byte[]> getByteRange(String bucketName, String blobKey, long startInclusive, long endInclusive) {
        long requestedLength = ByteArrayRangeValidator.validateAndGetLength(startInclusive, endInclusive);
        return exceptionHandler.handleAsync(
                CompletableFuture.supplyAsync(() -> {
                    requireBlob(bucketName, blobKey);
                    try (ReadChannel readChannel = client.reader(
                            BlobId.of(bucketName, blobKey),
                            Storage.BlobSourceOption.shouldReturnRawInputStream(true)
                    );
                         ByteArrayOutputStream output = new ByteArrayOutputStream();
                         WritableByteChannel outputChannel = Channels.newChannel(output)) {
                        readChannel.setChunkSize((int) Math.min(8192L, requestedLength));
                        readChannel.seek(startInclusive);

                        long remaining = requestedLength;
                        ByteBuffer buffer = ByteBuffer.allocate((int) Math.min(8192L, requestedLength));
                        while (remaining > 0) {
                            buffer.clear();
                            if (buffer.capacity() > remaining) {
                                buffer.limit((int) remaining);
                            }
                            int read = readChannel.read(buffer);
                            if (read <= 0) {
                                break;
                            }
                            buffer.flip();
                            while (buffer.hasRemaining()) {
                                outputChannel.write(buffer);
                            }
                            remaining -= read;
                        }
                        return output.toByteArray();
                    } catch (Exception error) {
                        throw new CompletionException(error);
                    }
                }, IO_EXECUTOR)
        );
    }

    @Override
    public URL generateGetUrl(String bucket, String objectKey, Duration expiry) {
        GCPClientSupport.validateExpiry(expiry);
        return exceptionHandler.handle(() -> GCPClientSupport.generateGetUrl(client, bucket, objectKey, expiry));
    }

    @Override
    public URL generatePutUrl(String bucket, String objectKey, Duration expiry) {
        GCPClientSupport.validateExpiry(expiry);
        return exceptionHandler.handle(() -> GCPClientSupport.generatePutUrl(client, bucket, objectKey, expiry));
    }

    private ApiFuture<BlobInfo> writeBlobAsync(BlobInfo blobInfo, byte[] content, Storage.BlobWriteOption... writeOptions) {
        return exceptionHandler.handle(() -> {
            BlobWriteSession writeSession = client.blobWriteSession(blobInfo, writeOptions);
            try (WritableByteChannel channel = writeSession.open()) {
                ByteBuffer buffer = ByteBuffer.wrap(content);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
            }
            return writeSession.getResult();
        });
    }

    private ApiFuture<BlobInfo> writeBlobAsync(BlobInfo blobInfo, byte[] content) {
        return writeBlobAsync(blobInfo, content, new Storage.BlobWriteOption[0]);
    }

    private ApiFuture<BlobInfo> writeBlobAsync(BlobInfo blobInfo, Flow.Publisher<ByteBuffer> content, long contentLength) {
        return exceptionHandler.handle(() -> {
            BlobWriteSession writeSession = client.blobWriteSession(blobInfo);
            try (WritableByteChannel channel = writeSession.open()) {
                GCPFlowPublisherChannelWriter.writeFromPublisher(content, channel, contentLength);
            } catch (Throwable error) {
                deleteBlobQuietly(blobInfo.getBucket(), blobInfo.getName());
                throw error;
            }
            return writeSession.getResult();
        });
    }

    private String createBlobFromFile(BlobInfo blobInfo, Path sourceFile) {
        return exceptionHandler.handle(
                () -> client.createFrom(blobInfo, sourceFile).getEtag()
        );
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

    private BlobInfo buildBlobInfo(String bucketName, io.github.michaelcirkl.ubsa.Blob blob) {
        BlobInfo.Builder blobBuilder = BlobInfo.newBuilder(bucketName, blob.getKey());
        WriteOptionsMappers.applyBlobToGcpBlobInfo(blobBuilder, blob);
        return blobBuilder.build();
    }

    private BlobInfo buildBlobInfo(String bucketName, String blobKey, BlobWriteOptions options) {
        BlobInfo.Builder blobBuilder = BlobInfo.newBuilder(bucketName, blobKey);
        WriteOptionsMappers.applyOptionsToGcpBlobInfo(blobBuilder, options);
        return blobBuilder.build();
    }

    private com.google.cloud.storage.Blob requireBlob(String bucketName, String blobKey) {
        com.google.cloud.storage.Blob blob = client.get(bucketName, blobKey);
        if (blob == null) {
            throw new StorageException(404, "Blob not found: gs://" + bucketName + "/" + blobKey);
        }
        return blob;
    }

    private void deleteBlobQuietly(String bucketName, String blobKey) {
        try {
            client.delete(bucketName, blobKey);
        } catch (StorageException ignored) {
        }
    }

    private void closeQuietly(BlobReadSession session) {
        exceptionHandler.closeQuietly(session::close);
    }
}
