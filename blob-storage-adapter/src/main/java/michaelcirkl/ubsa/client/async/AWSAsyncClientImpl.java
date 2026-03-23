package michaelcirkl.ubsa.client.async;


import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.*;
import michaelcirkl.ubsa.client.exception.AWSExceptionHandler;
import michaelcirkl.ubsa.client.streaming.*;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

public class AWSAsyncClientImpl implements BlobStorageAsyncClient {
    private static final String PATH_STYLE_PROBE_BUCKET = "ubsa-path-style-probe";
    private static final String PATH_STYLE_PROBE_KEY = "probe";

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
                        .thenApply(responseBytes -> buildBlobFromGetObject(bucketName, blobKey, responseBytes))
        );
    }

    @Override
    public CompletableFuture<Flow.Publisher<ByteBuffer>> openBlobStream(String bucketName, String blobKey) {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(blobKey)
                .build();
        return exceptionHandler.handleAsync(
                client.getObject(request, AsyncResponseTransformer.toPublisher())
                        .thenApply(FlowPublisherBridge::toFlowPublisher)
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
        return exceptionHandler.handleAsync(client.listBuckets().thenApply(this::mapBuckets));
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
        validateExpiry(expiry);
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucket)
                .key(objectKey)
                .build();
        GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                .signatureDuration(expiry)
                .getObjectRequest(getObjectRequest)
                .build();
        try (S3Presigner presigner = createPresignerFromClientConfig()) {
            PresignedGetObjectRequest presigned = presigner.presignGetObject(presignRequest);
            return presigned.url();
        } catch (S3Exception error) {
            throw exceptionHandler.wrap(error);
        }
    }

    @Override
    public URL generatePutUrl(String bucket, String objectKey, Duration expiry, String contentType) {
        validateExpiry(expiry);
        PutObjectRequest.Builder putBuilder = PutObjectRequest.builder()
                .bucket(bucket)
                .key(objectKey);
        if (contentType != null && !contentType.isBlank()) {
            putBuilder.contentType(contentType);
        }
        PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder()
                .signatureDuration(expiry)
                .putObjectRequest(putBuilder.build())
                .build();
        try (S3Presigner presigner = createPresignerFromClientConfig()) {
            PresignedPutObjectRequest presigned = presigner.presignPutObject(presignRequest);
            return presigned.url();
        } catch (S3Exception error) {
            throw exceptionHandler.wrap(error);
        }
    }

    private List<Bucket> mapBuckets(ListBucketsResponse response) {
        List<Bucket> buckets = new ArrayList<>();
        response.buckets().forEach(bucket -> {
            LocalDateTime creation = toLocalDateTime(bucket.creationDate());
            buckets.add(Bucket.builder()
                    .name(bucket.name())
                    .publicURI(toS3Uri(bucket.name(), null))
                    .creationDate(creation)
                    .lastModified(creation)
                    .build());
        });
        return buckets;
    }

    private List<Blob> mapBlobsFromList(String bucketName, ListObjectsV2Response response) {
        List<Blob> blobs = new ArrayList<>();
        response.contents().forEach(object -> {
            blobs.add(Blob.builder()
                    .bucket(bucketName)
                    .key(object.key())
                    .size(object.size())
                    .lastModified(toLocalDateTime(object.lastModified()))
                    .etag(object.eTag())
                    .publicURI(toS3Uri(bucketName, object.key()))
                    .build());
        });
        return blobs;
    }

    private CompletableFuture<List<Blob>> listAllBlobs(String bucketName, ListObjectsV2Request request) {
        return client.listObjectsV2(request)
                .thenCompose(response -> {
                    List<Blob> blobs = mapBlobsFromList(bucketName, response);
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

    private Blob buildBlobFromGetObject(String bucketName, String blobKey, ResponseBytes<GetObjectResponse> responseBytes) {
        GetObjectResponse response = responseBytes.response();
        return Blob.builder()
                .bucket(bucketName)
                .key(blobKey)
                .content(responseBytes.asByteArray())
                .size(response.contentLength())
                .lastModified(toLocalDateTime(response.lastModified()))
                .encoding(response.contentEncoding())
                .etag(response.eTag())
                .userMetadata(response.metadata())
                .publicURI(toS3Uri(bucketName, blobKey))
                .expires(parseExpiresHeader(response.expiresString()))
                .build();
    }

    private LocalDateTime toLocalDateTime(Instant instant) {
        return instant == null ? null : LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private URI toS3Uri(String bucketName, String key) {
        String uri = (key == null || key.isBlank())
                ? "s3://" + bucketName
                : "s3://" + bucketName + "/" + key;
        return URI.create(uri);
    }

    private boolean isPreconditionFailed(Throwable error) {
        return error instanceof S3Exception s3Exception
                && s3Exception.statusCode() == 412;
    }

    private LocalDateTime parseExpiresHeader(String expiresHeader) {
        if (expiresHeader == null || expiresHeader.isBlank()) {
            return null;
        }
        try {
            ZonedDateTime expires = ZonedDateTime.parse(expiresHeader, DateTimeFormatter.RFC_1123_DATE_TIME);
            return expires.withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime();
        } catch (DateTimeParseException ignored) {
            return null;
        }
    }

    private void validateExpiry(Duration expiry) {
        if (expiry == null || expiry.isZero() || expiry.isNegative()) {
            throw new IllegalArgumentException("Expiry must be a positive duration.");
        }
    }

    private void validateAwsSinglePutLength(long contentLength) {
        long maxSinglePutBytes = 5L * 1024L * 1024L * 1024L;
        if (contentLength > maxSinglePutBytes) {
            throw new IllegalArgumentException("AWS single PUT upload supports up to 5 GiB. Received: " + contentLength + " bytes.");
        }
    }

    private S3Presigner createPresignerFromClientConfig() {
        S3ServiceClientConfiguration config = client.serviceClientConfiguration();
        S3Presigner.Builder builder = S3Presigner.builder()
                .region(config.region())
                .credentialsProvider(config.credentialsProvider())
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(isPathStyleEnabled())
                        .build());
        config.endpointOverride().ifPresent(builder::endpointOverride);
        return builder.build();
    }

    private boolean isPathStyleEnabled() {
        try {
            URL probeUrl = client.utilities().getUrl(GetUrlRequest.builder()
                    .bucket(PATH_STYLE_PROBE_BUCKET)
                    .key(PATH_STYLE_PROBE_KEY)
                    .build());
            String expectedPrefix = "/" + PATH_STYLE_PROBE_BUCKET + "/";
            return probeUrl.getPath() != null && probeUrl.getPath().startsWith(expectedPrefix);
        } catch (RuntimeException ignored) {
            // Fallback to default virtual-host style if probing fails.
            return false;
        }
    }
}
