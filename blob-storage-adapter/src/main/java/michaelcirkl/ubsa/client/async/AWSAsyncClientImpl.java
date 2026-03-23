package michaelcirkl.ubsa.client.async;


import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.*;
import michaelcirkl.ubsa.client.util.AwsClientSupport;
import michaelcirkl.ubsa.client.exception.AWSExceptionHandler;
import michaelcirkl.ubsa.client.streaming.*;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

public class AWSAsyncClientImpl implements BlobStorageAsyncClient {
    private final AWSExceptionHandler exceptionHandler = new AWSExceptionHandler();
    private final S3AsyncClient client;

    public AWSAsyncClientImpl(S3AsyncClient client) {
        this.client = client;
    }


    @Override
    public Provider getProvider() {
        return Provider.AWS;
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
        HeadBucketRequest request = HeadBucketRequest.builder().bucket(bucketName).build();
        return handleExistsCheck(client.headBucket(request));
    }

    @Override
    public CompletableFuture<Blob> getBlob(String bucketName, String blobKey) {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(blobKey)
                .build();

        return exceptionHandler.handleAsync(
                client.getObject(request, AsyncResponseTransformer.toBytes())
                        .thenApply(responseBytes -> AwsClientSupport.buildBlobFromGetObject(bucketName, blobKey, responseBytes))
        );
    }

    @Override
    public Flow.Publisher<ByteBuffer> openBlobStream(String bucketName, String blobKey) {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(blobKey)
                .build();
        return new DeferredFlowPublisher<>(
                exceptionHandler.handleAsync(
                        client.getObject(request, AsyncResponseTransformer.toPublisher())
                                .thenApply(FlowPublisherBridge::toFlowPublisher)
                ),
                exceptionHandler::propagate
        );
    }

    @Override
    public CompletableFuture<Void> deleteBucket(String bucketName) {
        DeleteBucketRequest request = DeleteBucketRequest.builder()
                .bucket(bucketName)
                .build();
        return exceptionHandler.handleAsync(client.deleteBucket(request).thenApply(response -> null));
    }

    @Override
    public CompletableFuture<Boolean> blobExists(String bucketName, String blobKey) {
        HeadObjectRequest request = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(blobKey)
                .build();
        return handleExistsCheck(client.headObject(request));
    }

    @Override
    public CompletableFuture<String> createBlob(String bucketName, Blob blob) {
        PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(blob.getKey());
        WriteOptionsMappers.applyBlobToAwsPutObject(requestBuilder, blob);

        byte[] content = blob.getContent() == null ? new byte[0] : blob.getContent();
        PutObjectRequest request = requestBuilder.build();
        return exceptionHandler.handleAsync(
                client.putObject(request, AsyncRequestBody.fromBytes(content))
                        .thenApply(PutObjectResponse::eTag)
        );
    }

    @Override
    public CompletableFuture<String> createBlob(String bucketName, String blobKey, Path sourceFile) {
        FileUploadValidators.validateSourceFile(sourceFile);
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(blobKey)
                .build();
        return exceptionHandler.handleAsync(
                client.putObject(request, AsyncRequestBody.fromFile(sourceFile))
                        .thenApply(PutObjectResponse::eTag)
        );
    }

    @Override
    public CompletableFuture<String> createBlob(String bucketName, String blobKey, Flow.Publisher<ByteBuffer> content, long contentLength, BlobWriteOptions options) {
        ContentLengthValidators.validateContentLength(contentLength);
        if (content == null) {
            throw new IllegalArgumentException("Content publisher must not be null.");
        }
        validateAwsSinglePutLength(contentLength);
        PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(blobKey)
                .contentLength(contentLength);
        WriteOptionsMappers.applyOptionsToAwsPutObject(requestBuilder, options);
        return exceptionHandler.handleAsync(
                client.putObject(
                                requestBuilder.build(),
                                AsyncRequestBody.fromPublisher(FlowPublisherBridge.toReactivePublisher(content))
                        )
                        .thenApply(PutObjectResponse::eTag)
        );
    }

    @Override
    public CompletableFuture<Void> deleteBlobIfExists(String bucketName, String blobKey) {
        DeleteObjectRequest request = DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(blobKey)
                .build();
        return exceptionHandler.handleAsync(client.deleteObject(request).thenApply(response -> null));
    }

    @Override
    public CompletableFuture<String> copyBlob(String sourceBucketName, String sourceBlobKey, String destinationBucketName, String destinationBlobKey) {
        CopyObjectRequest request = CopyObjectRequest.builder()
                .sourceBucket(sourceBucketName)
                .sourceKey(sourceBlobKey)
                .destinationBucket(destinationBucketName)
                .destinationKey(destinationBlobKey)
                .build();
        return exceptionHandler.handleAsync(
                client.copyObject(request)
                        .thenApply(CopyObjectResponse::copyObjectResult)
                        .thenApply(result -> result == null ? null : result.eTag())
        );
    }

    @Override
    public CompletableFuture<List<Bucket>> listAllBuckets() {
        return exceptionHandler.handleAsync(client.listBuckets().thenApply(AwsClientSupport::mapBuckets));
    }

    @Override
    public CompletableFuture<List<Blob>> listBlobsByPrefix(String bucketName, String prefix) {
        ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                .bucket(bucketName);
        if (prefix != null && !prefix.isBlank()) {
            requestBuilder.prefix(prefix);
        }
        return exceptionHandler.handleAsync(listAllBlobs(bucketName, requestBuilder.build()));
    }

    @Override
    public CompletableFuture<Void> createBucket(Bucket bucket) {
        CreateBucketRequest request = CreateBucketRequest.builder()
                .bucket(bucket.getName())
                .build();
        return exceptionHandler.handleAsync(client.createBucket(request).thenApply(response -> null));
    }

    @Override
    public CompletableFuture<List<Blob>> getAllBlobsInBucket(String bucketName) {
        return listBlobsByPrefix(bucketName, null);
    }

    @Override
    public CompletableFuture<Void> deleteBucketIfExists(String bucketName) {
        DeleteBucketRequest request = DeleteBucketRequest.builder()
                .bucket(bucketName)
                .build();
        return client.deleteBucket(request)
                .handle((response, error) -> {
                    if (error == null) {
                        return null;
                    }

                    Throwable cause = exceptionHandler.unwrap(error);
                    if (cause instanceof S3Exception s3Exception) {
                        if (exceptionHandler.isNotFound(s3Exception)) {
                            return null;
                        }
                        throw exceptionHandler.wrap(s3Exception);
                    }
                    throw exceptionHandler.propagate(cause);
                });
    }

    @Override
    public CompletableFuture<byte[]> getByteRange(String bucketName, String blobKey, long startInclusive, long endInclusive) {
        validateRange(startInclusive, endInclusive);
        String range = "bytes=" + startInclusive + "-" + endInclusive;
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(blobKey)
                .range(range)
                .build();
        return exceptionHandler.handleAsync(
                client.getObject(request, AsyncResponseTransformer.toBytes())
                        .thenApply(ResponseBytes::asByteArray)
        );
    }

    @Override
    public URL generateGetUrl(String bucket, String objectKey, Duration expiry) {
        AwsClientSupport.validateExpiry(expiry);
        return exceptionHandler.handle(() -> AwsClientSupport.presignGetUrl(bucket, objectKey, expiry, this::createPresignerFromClientConfig));
    }

    @Override
    public URL generatePutUrl(String bucket, String objectKey, Duration expiry, String contentType) {
        AwsClientSupport.validateExpiry(expiry);
        return exceptionHandler.handle(() -> AwsClientSupport.presignPutUrl(bucket, objectKey, expiry, contentType, this::createPresignerFromClientConfig));
    }

    private CompletableFuture<List<Blob>> listAllBlobs(String bucketName, ListObjectsV2Request request) {
        return client.listObjectsV2(request)
                .thenCompose(response -> {
                    List<Blob> blobs = AwsClientSupport.mapBlobsFromList(bucketName, response);
                    if (!response.isTruncated()) {
                        return CompletableFuture.completedFuture(blobs);
                    }

                    ListObjectsV2Request nextRequest = request.toBuilder()
                            .continuationToken(response.nextContinuationToken())
                            .build();
                    return listAllBlobs(bucketName, nextRequest)
                            .thenApply(nextPageBlobs -> {
                                blobs.addAll(nextPageBlobs);
                                return blobs;
                            });
                });
    }

    private <T> CompletableFuture<Boolean> handleExistsCheck(CompletableFuture<T> future) {
        return future.handle((result, error) -> {
            if (error == null) {
                return true;
            }

            Throwable cause = exceptionHandler.unwrap(error);
            if (cause instanceof S3Exception s3Exception) {
                if (exceptionHandler.isNotFound(s3Exception)) {
                    return false;
                }
                throw exceptionHandler.wrap(s3Exception);
            }
            throw exceptionHandler.propagate(cause);
        });
    }

    private void validateAwsSinglePutLength(long contentLength) {
        long maxSinglePutBytes = 5L * 1024L * 1024L * 1024L;
        if (contentLength > maxSinglePutBytes) {
            throw new IllegalArgumentException("AWS single PUT upload supports up to 5 GiB. Received: " + contentLength + " bytes.");
        }
    }

    private void validateRange(long startInclusive, long endInclusive) {
        if (startInclusive < 0 || endInclusive < startInclusive) {
            throw new IllegalArgumentException("Invalid range. startInclusive must be >= 0 and endInclusive must be >= startInclusive.");
        }
    }

    private software.amazon.awssdk.services.s3.presigner.S3Presigner createPresignerFromClientConfig() {
        return AwsClientSupport.createPresignerFromClientConfig(
                client.serviceClientConfiguration(),
                () -> client.utilities().getUrl(AwsClientSupport.pathStyleProbeRequest())
        );
    }
}
