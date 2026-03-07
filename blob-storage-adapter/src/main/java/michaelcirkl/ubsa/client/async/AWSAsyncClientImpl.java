package michaelcirkl.ubsa.client.async;


import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.*;
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
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;

public class AWSAsyncClientImpl implements BlobStorageAsyncClient {
    private static final String PATH_STYLE_PROBE_BUCKET = "ubsa-path-style-probe";
    private static final String PATH_STYLE_PROBE_KEY = "probe";

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
        return client.headBucket(request)
                .handle((response, error) -> {
                    if (error == null) {
                        return true;
                    }
                    Throwable cause = StreamErrorAdapters.unwrapCompletionException(error);
                    if (isNotFound(cause)) {
                        return false;
                    }
                    throw toCompletionException("Failed to check whether AWS bucket exists: " + bucketName, cause);
                });
    }

    @Override
    public CompletableFuture<Blob> getBlob(String bucketName, String blobKey) {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(blobKey)
                .build();

        return wrapS3Exception(
                client.getObject(request, AsyncResponseTransformer.toBytes())
                        .thenApply(responseBytes -> buildBlobFromGetObject(bucketName, blobKey, responseBytes)),
                "Failed to get AWS blob s3://" + bucketName + "/" + blobKey
        );
    }

    @Override
    public CompletableFuture<Flow.Publisher<ByteBuffer>> openBlobStream(String bucketName, String blobKey) {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(blobKey)
                .build();
        return wrapS3Exception(
                client.getObject(request, AsyncResponseTransformer.toPublisher())
                        .thenApply(FlowPublisherBridge::toFlowPublisher),
                "Failed to open AWS blob stream s3://" + bucketName + "/" + blobKey
        );
    }

    @Override
    public CompletableFuture<Void> deleteBucket(String bucketName) {
        DeleteBucketRequest request = DeleteBucketRequest.builder()
                .bucket(bucketName)
                .build();
        return wrapS3Exception(
                client.deleteBucket(request).thenApply(response -> null),
                "Failed to delete AWS bucket: " + bucketName
        );
    }

    @Override
    public CompletableFuture<Boolean> blobExists(String bucketName, String blobKey) {
        HeadObjectRequest request = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(blobKey)
                .build();
        return client.headObject(request)
                .handle((response, error) -> {
                    if (error == null) {
                        return true;
                    }
                    Throwable cause = StreamErrorAdapters.unwrapCompletionException(error);
                    if (isNotFound(cause)) {
                        return false;
                    }
                    throw toCompletionException(
                            "Failed to check whether AWS blob exists: s3://" + bucketName + "/" + blobKey,
                            cause
                    );
                });
    }

    @Override
    public CompletableFuture<String> createBlob(String bucketName, Blob blob) {
        PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(blob.getKey());
        WriteOptionsMappers.applyBlobToAwsPutObject(requestBuilder, blob);

        byte[] content = blob.getContent() == null ? new byte[0] : blob.getContent();
        PutObjectRequest request = requestBuilder.build();
        return wrapS3Exception(
                client.putObject(request, AsyncRequestBody.fromBytes(content))
                        .thenApply(PutObjectResponse::eTag),
                "Failed to create AWS blob s3://" + bucketName + "/" + blob.getKey()
        );
    }

    @Override
    public CompletableFuture<String> createBlob(String bucketName, String blobKey, Flow.Publisher<ByteBuffer> content, long contentLength, BlobWriteOptions options) {
        ContentLengthValidators.validateContentLength(contentLength);
        if (content == null) {
            throw new IllegalArgumentException("Content publisher must not be null.");
        }
        validateAwsSinglePutLength(contentLength);
        Flow.Publisher<ByteBuffer> lengthCheckedContent = ContentLengthValidators.enforcePublisherContentLength(content, contentLength);
        PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(blobKey)
                .contentLength(contentLength);
        WriteOptionsMappers.applyOptionsToAwsPutObject(requestBuilder, options);
        return wrapS3Exception(
                client.putObject(
                                requestBuilder.build(),
                                AsyncRequestBody.fromPublisher(FlowPublisherBridge.toReactivePublisher(lengthCheckedContent))
                        )
                        .thenApply(PutObjectResponse::eTag),
                "Failed to stream-create AWS blob s3://" + bucketName + "/" + blobKey
        );
    }

    @Override
    public CompletableFuture<Void> deleteBlob(String bucketName, String blobKey) {
        DeleteObjectRequest request = DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(blobKey)
                .build();
        return wrapS3Exception(
                client.deleteObject(request).thenApply(response -> null),
                "Failed to delete AWS blob s3://" + bucketName + "/" + blobKey
        );
    }

    @Override
    public CompletableFuture<String> copyBlob(String sourceBucketName, String sourceBlobKey, String destinationBucketName, String destinationBlobKey) {
        CopyObjectRequest request = CopyObjectRequest.builder()
                .sourceBucket(sourceBucketName)
                .sourceKey(sourceBlobKey)
                .destinationBucket(destinationBucketName)
                .destinationKey(destinationBlobKey)
                .build();
        return wrapS3Exception(
                client.copyObject(request)
                        .thenApply(CopyObjectResponse::copyObjectResult)
                        .thenApply(result -> result == null ? null : result.eTag()),
                "Failed to copy AWS blob from s3://" + sourceBucketName + "/" + sourceBlobKey
                        + " to s3://" + destinationBucketName + "/" + destinationBlobKey
        );
    }

    @Override
    public CompletableFuture<Set<Bucket>> listAllBuckets() {
        return wrapS3Exception(
                client.listBuckets().thenApply(this::mapBuckets),
                "Failed to list AWS buckets"
        );
    }

    @Override
    public CompletableFuture<Set<Blob>> listBlobsByPrefix(String bucketName, String prefix) {
        ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                .bucket(bucketName);
        if (prefix != null && !prefix.isBlank()) {
            requestBuilder.prefix(prefix);
        }
        return wrapS3Exception(
                client.listObjectsV2(requestBuilder.build())
                        .thenApply(response -> mapBlobsFromList(bucketName, response)),
                "Failed to list AWS blobs in bucket " + bucketName
        );
    }

    @Override
    public CompletableFuture<Void> createBucket(Bucket bucket) {
        CreateBucketRequest request = CreateBucketRequest.builder()
                .bucket(bucket.getName())
                .build();
        return wrapS3Exception(
                client.createBucket(request).thenApply(response -> null),
                "Failed to create AWS bucket " + bucket.getName()
        );
    }

    @Override
    public CompletableFuture<Set<Blob>> getAllBlobsInBucket(String bucketName) {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .build();
        return wrapS3Exception(
                client.listObjectsV2(request)
                        .thenApply(response -> mapBlobsFromList(bucketName, response)),
                "Failed to list all AWS blobs in bucket " + bucketName
        );
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
                    Throwable cause = StreamErrorAdapters.unwrapCompletionException(error);
                    if (isNotFound(cause)) {
                        return null;
                    }
                    throw toCompletionException("Failed to delete AWS bucket if exists: " + bucketName, cause);
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
        return wrapS3Exception(
                client.getObject(request, AsyncResponseTransformer.toBytes())
                        .thenApply(ResponseBytes::asByteArray),
                "Failed to read byte range from AWS blob s3://" + bucketName + "/" + blobKey
        );
    }

    @Override
    public CompletableFuture<String> createBlobIfNotExists(String bucketName, Blob blob) {
        HeadObjectRequest headRequest = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(blob.getKey())
                .build();
        CompletableFuture<String> existing = client.headObject(headRequest)
                .handle((response, error) -> {
                    if (error == null) {
                        return response.eTag();
                    }
                    Throwable cause = StreamErrorAdapters.unwrapCompletionException(error);
                    if (isNotFound(cause)) {
                        return null;
                    }
                    throw toCompletionException(
                            "Failed to create AWS blob if not exists: s3://" + bucketName + "/" + blob.getKey(),
                            cause
                    );
                });
        return existing.thenCompose(etag -> {
            if (etag != null) {
                return CompletableFuture.completedFuture(etag);
            }
            return createBlob(bucketName, blob);
        });
    }

    @Override
    public CompletableFuture<URL> generateGetUrl(String bucket, String objectKey, Duration expiry) {
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
            return CompletableFuture.completedFuture(presigned.url());
        } catch (S3Exception error) {
            throw new UbsaException("Failed to generate AWS GET URL for s3://" + bucket + "/" + objectKey, error);
        }
    }

    @Override
    public CompletableFuture<URL> generatePutUrl(String bucket, String objectKey, Duration expiry, String contentType) {
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
            return CompletableFuture.completedFuture(presigned.url());
        } catch (S3Exception error) {
            throw new UbsaException("Failed to generate AWS PUT URL for s3://" + bucket + "/" + objectKey, error);
        }
    }

    private Set<Bucket> mapBuckets(ListBucketsResponse response) {
        Set<Bucket> buckets = new HashSet<>();
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

    private Set<Blob> mapBlobsFromList(String bucketName, ListObjectsV2Response response) {
        Set<Blob> blobs = new HashSet<>();
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

    private boolean isNotFound(Throwable error) {
        if (error instanceof NoSuchBucketException || error instanceof NoSuchKeyException) {
            return true;
        }
        return error instanceof S3Exception s3Exception
                && s3Exception.statusCode() == 404;
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

    private <T> CompletableFuture<T> wrapS3Exception(CompletableFuture<T> future, String message) {
        return StreamErrorAdapters.wrapUbsaFuture(future, message, S3Exception.class);
    }

    private CompletionException toCompletionException(String message, Throwable cause) {
        return StreamErrorAdapters.toCompletionException(message, cause, S3Exception.class);
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
