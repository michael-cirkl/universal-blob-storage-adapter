package michaelcirkl.ubsa.aws;


import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.BlobStorageClient;
import michaelcirkl.ubsa.Bucket;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

public class AWSClientImpl implements BlobStorageClient {
    private final S3AsyncClient client;
    private final String endpoint;
    public AWSClientImpl(String endpoint, String accessKey, String secretKey, String region) {
        this.endpoint = endpoint;
        client = S3AsyncClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.of(region))
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)))
                .build();
    }

    @Override
    public CompletableFuture<Boolean> bucketExists(String bucketName) {
        return client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build())
                .thenApply(response -> true)
                .exceptionally(error -> {
                    if (isNotFound(error)) {
                        return false;
                    }
                    throw new CompletionException(error);
                });
    }

    @Override
    public CompletableFuture<Blob> getBlob(String bucketName, String blobKey) {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(blobKey)
                .build();
        return client.getObject(request, AsyncResponseTransformer.toBytes())
                .thenApply(responseBytes -> {
                    byte[] content = responseBytes.asByteArray();
                    var response = responseBytes.response();
                    long size = response.contentLength() == null ? content.length : response.contentLength();
                    return Blob.builder()
                            .content(content)
                            .size(size)
                            .key(blobKey)
                            .lastModified(toLocalDateTime(response.lastModified()))
                            .encoding(response.contentEncoding())
                            .contentMD5(parseMd5(response.eTag()))
                            .etag(response.eTag())
                            .userMetadata(response.metadata())
                            .publicURI(buildBlobUri(bucketName, blobKey))
                            .bucket(bucketName)
                            .expires(toLocalDateTime(response.expires()))
                            .build();
                });
    }

    @Override
    public CompletableFuture<Void> deleteBucket(String bucketName) {
        return client.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build())
                .thenApply(response -> null);
    }

    @Override
    public CompletableFuture<Boolean> blobExists(String bucketName, String blobKey) {
        return client.headObject(HeadObjectRequest.builder().bucket(bucketName).key(blobKey).build())
                .thenApply(response -> true)
                .exceptionally(error -> {
                    if (isNotFound(error)) {
                        return false;
                    }
                    throw new CompletionException(error);
                });
    }

    @Override
    public CompletableFuture<String> createBlob(String bucketName, Blob blob) {
        String key = Objects.requireNonNull(blob.getKey(), "Blob key must not be null");
        byte[] content = blob.getContent() == null ? new byte[0] : blob.getContent();
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        return client.putObject(request, AsyncRequestBody.fromBytes(content))
                .thenApply(response -> key);
    }

    @Override
    public CompletableFuture<Void> deleteBlob(String bucketName, String blobKey) {
        return client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(blobKey).build())
                .thenApply(response -> null);
    }

    @Override
    public CompletableFuture<String> copyBlob(String sourceBucketName, String sourceBlobKey, String destinationBucketName, String destinationBlobKey) {
        String copySource = sourceBucketName + "/" + sourceBlobKey;
        CopyObjectRequest request = CopyObjectRequest.builder()
                .copySource(copySource)
                .bucket(destinationBucketName)
                .key(destinationBlobKey)
                .build();
        return client.copyObject(request)
                .thenApply(response -> destinationBlobKey);
    }

    @Override
    public CompletableFuture<Set<Bucket>> listAllBuckets() {
        return client.listBuckets(ListBucketsRequest.builder().build())
                .thenApply(response -> response.buckets().stream()
                        .map(bucket -> Bucket.builder()
                                .name(bucket.name())
                                .publicURI(buildBucketUri(bucket.name()))
                                .creationDate(toLocalDateTime(bucket.creationDate()))
                                .build())
                        .collect(Collectors.toCollection(LinkedHashSet::new)));
    }

    @Override
    public CompletableFuture<Set<Blob>> listBlobsByPrefix(String bucketName, String prefix) {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(prefix)
                .build();
        return client.listObjectsV2(request)
                .thenApply(response -> response.contents().stream()
                        .map(s3Object -> toBlob(bucketName, s3Object))
                        .collect(Collectors.toCollection(LinkedHashSet::new)));
    }

    @Override
    public CompletableFuture<Void> createBucket(Bucket bucket) {
        return client.createBucket(CreateBucketRequest.builder().bucket(bucket.getName()).build())
                .thenApply(response -> null);
    }

    @Override
    public CompletableFuture<Set<Blob>> getAllBlobsInBucket(String bucketName) {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .build();
        return client.listObjectsV2(request)
                .thenApply(response -> response.contents().stream()
                        .map(s3Object -> toBlob(bucketName, s3Object))
                        .collect(Collectors.toCollection(LinkedHashSet::new)));
    }

    @Override
    public CompletableFuture<Void> deleteBucketIfExists(String bucketName) {
        return bucketExists(bucketName)
                .thenCompose(exists -> exists ? deleteBucket(bucketName) : CompletableFuture.completedFuture(null));
    }

    @Override
    public CompletableFuture<byte[]> getByteRange(String bucketName, String blobKey, long startInclusive, long endInclusive) {
        String range = "bytes=" + startInclusive + "-" + endInclusive;
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(blobKey)
                .range(range)
                .build();
        return client.getObject(request, AsyncResponseTransformer.toBytes())
                .thenApply(responseBytes -> responseBytes.asByteArray());
    }

    @Override
    public CompletableFuture<String> createBlobIfNotExists(String bucketName, Blob blob) {
        String key = Objects.requireNonNull(blob.getKey(), "Blob key must not be null");
        return blobExists(bucketName, key)
                .thenCompose(exists -> exists
                        ? CompletableFuture.completedFuture(key)
                        : createBlob(bucketName, blob));
    }

    private Blob toBlob(String bucketName, S3Object s3Object) {
        return Blob.builder()
                .content(null)
                .size(s3Object.size() == null ? 0L : s3Object.size())
                .key(s3Object.key())
                .lastModified(toLocalDateTime(s3Object.lastModified()))
                .encoding(null)
                .contentMD5(parseMd5(s3Object.eTag()))
                .etag(s3Object.eTag())
                .userMetadata(Collections.emptyMap())
                .publicURI(buildBlobUri(bucketName, s3Object.key()))
                .bucket(bucketName)
                .expires(null)
                .build();
    }

    private URI buildBlobUri(String bucketName, String blobKey) {
        if (endpoint == null || endpoint.isBlank()) {
            return null;
        }
        String base = endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
        return URI.create(base + "/" + bucketName + "/" + blobKey);
    }

    private URI buildBucketUri(String bucketName) {
        if (endpoint == null || endpoint.isBlank()) {
            return null;
        }
        String base = endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
        return URI.create(base + "/" + bucketName);
    }

    private boolean isNotFound(Throwable error) {
        Throwable cause = unwrapCompletionException(error);
        if (cause instanceof NoSuchBucketException || cause instanceof NoSuchKeyException) {
            return true;
        }
        if (cause instanceof S3Exception s3Exception) {
            int status = s3Exception.statusCode();
            if (status == 404) {
                return true;
            }
            return "NoSuchBucket".equals(s3Exception.awsErrorDetails().errorCode())
                    || "NoSuchKey".equals(s3Exception.awsErrorDetails().errorCode());
        }
        return false;
    }

    private  Throwable unwrapCompletionException(Throwable error) {
        if (error instanceof CompletionException completionException && completionException.getCause() != null) {
            return completionException.getCause();
        }
        return error;
    }

    private LocalDateTime toLocalDateTime(Instant instant) {
        return instant == null ? null : LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private String parseMd5(String eTag) {
        if (eTag == null) {
            return null;
        }
        String normalized = eTag.replace("\"", "");
        if (!normalized.matches("^[a-fA-F0-9]{32}$")) {
            return null;
        }
        return normalized.toLowerCase(Locale.ROOT);
    }
}
