package michaelcirkl.ubsa.impl.async;


import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.BlobStorageAsyncClient;
import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.Provider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public class AWSAsyncClientImpl implements BlobStorageAsyncClient {
    private final S3AsyncClient client;

    public AWSAsyncClientImpl(S3AsyncClient client) {
        this.client = client;
    }


    @Override
    public Provider getProvider() {
        return Provider.AWS;
    }

    @Override
    public CompletableFuture<Boolean> bucketExists(String bucketName) {
        HeadBucketRequest request = HeadBucketRequest.builder().bucket(bucketName).build();
        return client.headBucket(request)
                .handle((response, error) -> {
                    if (error == null) {
                        return true;
                    }
                    Throwable cause = unwrapCompletionException(error);
                    if (isNotFound(cause)) {
                        return false;
                    }
                    throw new CompletionException(cause);
                });
    }

    @Override
    public CompletableFuture<Blob> getBlob(String bucketName, String blobKey) {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(blobKey)
                .build();

        return client.getObject(request, AsyncResponseTransformer.toBytes())
                .thenApply(responseBytes -> buildBlobFromGetObject(bucketName, blobKey, responseBytes));
    }

    @Override
    public CompletableFuture<Void> deleteBucket(String bucketName) {
        DeleteBucketRequest request = DeleteBucketRequest.builder()
                .bucket(bucketName)
                .build();
        return client.deleteBucket(request).thenApply(response -> null);
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
                    Throwable cause = unwrapCompletionException(error);
                    if (isNotFound(cause)) {
                        return false;
                    }
                    throw new CompletionException(cause);
                });
    }

    @Override
    public CompletableFuture<String> createBlob(String bucketName, Blob blob) {
        PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(blob.getKey());

        if (blob.encoding() != null) {
            requestBuilder.contentEncoding(blob.encoding());
        }
        Map<String, String> metadata = blob.getUserMetadata();
        if (metadata != null && !metadata.isEmpty()) {
            requestBuilder.metadata(metadata);
        }
        if (blob.expires() != null) {
            requestBuilder.expires(blob.expires().toInstant(ZoneOffset.UTC));
        }

        byte[] content = blob.getContent() == null ? new byte[0] : blob.getContent();
        PutObjectRequest request = requestBuilder.build();
        return client.putObject(request, AsyncRequestBody.fromBytes(content))
                .thenApply(PutObjectResponse::eTag);
    }

    @Override
    public CompletableFuture<Void> deleteBlob(String bucketName, String blobKey) {
        DeleteObjectRequest request = DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(blobKey)
                .build();
        return client.deleteObject(request).thenApply(response -> null);
    }

    @Override
    public CompletableFuture<String> copyBlob(String sourceBucketName, String sourceBlobKey, String destinationBucketName, String destinationBlobKey) {
        String copySource = sourceBucketName + "/" + sourceBlobKey;
        CopyObjectRequest request = CopyObjectRequest.builder()
                .copySource(copySource)
                .destinationBucket(destinationBucketName)
                .destinationKey(destinationBlobKey)
                .build();
        return client.copyObject(request)
                .thenApply(CopyObjectResponse::copyObjectResult)
                .thenApply(result -> result == null ? null : result.eTag());
    }

    @Override
    public CompletableFuture<Set<Bucket>> listAllBuckets() {
        return client.listBuckets()
                .thenApply(this::mapBuckets);
    }

    @Override
    public CompletableFuture<Set<Blob>> listBlobsByPrefix(String bucketName, String prefix) {
        ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                .bucket(bucketName);
        if (prefix != null && !prefix.isBlank()) {
            requestBuilder.prefix(prefix);
        }
        return client.listObjectsV2(requestBuilder.build())
                .thenApply(response -> mapBlobsFromList(bucketName, response));
    }

    @Override
    public CompletableFuture<Void> createBucket(Bucket bucket) {
        CreateBucketRequest request = CreateBucketRequest.builder()
                .bucket(bucket.getName())
                .build();
        return client.createBucket(request).thenApply(response -> null);
    }

    @Override
    public CompletableFuture<Set<Blob>> getAllBlobsInBucket(String bucketName) {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .build();
        return client.listObjectsV2(request)
                .thenApply(response -> mapBlobsFromList(bucketName, response));
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
                    Throwable cause = unwrapCompletionException(error);
                    if (isNotFound(cause)) {
                        return null;
                    }
                    throw new CompletionException(cause);
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
        return client.getObject(request, AsyncResponseTransformer.toBytes())
                .thenApply(ResponseBytes::asByteArray);
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
                    Throwable cause = unwrapCompletionException(error);
                    if (isNotFound(cause)) {
                        return null;
                    }
                    throw new CompletionException(cause);
                });
        return existing.thenCompose(etag -> {
            if (etag != null) {
                return CompletableFuture.completedFuture(etag);
            }
            return createBlob(bucketName, blob);
        });
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
                .expires(toLocalDateTime(response.expires()))
                .build();
    }

    private static LocalDateTime toLocalDateTime(Instant instant) {
        return instant == null ? null : LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private static URI toS3Uri(String bucketName, String key) {
        String uri = (key == null || key.isBlank())
                ? "s3://" + bucketName
                : "s3://" + bucketName + "/" + key;
        return URI.create(uri);
    }

    private static Throwable unwrapCompletionException(Throwable error) {
        if (error instanceof CompletionException completionException && completionException.getCause() != null) {
            return completionException.getCause();
        }
        return error;
    }

    private static boolean isNotFound(Throwable error) {
        if (error instanceof NoSuchBucketException || error instanceof NoSuchKeyException) {
            return true;
        }
        return error instanceof software.amazon.awssdk.services.s3.model.S3Exception s3Exception
                && s3Exception.statusCode() == 404;
    }
}
