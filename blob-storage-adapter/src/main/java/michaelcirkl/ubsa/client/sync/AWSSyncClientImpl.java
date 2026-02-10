package michaelcirkl.ubsa.client.sync;

import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.BlobStorageSyncClient;
import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.Provider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.GetUrlRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AWSSyncClientImpl implements BlobStorageSyncClient {
    private static final String PATH_STYLE_PROBE_BUCKET = "ubsa-path-style-probe";
    private static final String PATH_STYLE_PROBE_KEY = "probe";

    private final S3Client client;

    public AWSSyncClientImpl(S3Client client) {
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
    public Boolean bucketExists(String bucketName) {
        HeadBucketRequest request = HeadBucketRequest.builder().bucket(bucketName).build();
        try {
            client.headBucket(request);
            return true;
        } catch (RuntimeException error) {
            if (isNotFound(error)) {
                return false;
            }
            throw error;
        }
    }

    @Override
    public Blob getBlob(String bucketName, String blobKey) {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(blobKey)
                .build();

        ResponseBytes<GetObjectResponse> responseBytes = client.getObjectAsBytes(request);
        return buildBlobFromGetObject(bucketName, blobKey, responseBytes);
    }

    @Override
    public Void deleteBucket(String bucketName) {
        DeleteBucketRequest request = DeleteBucketRequest.builder()
                .bucket(bucketName)
                .build();
        client.deleteBucket(request);
        return null;
    }

    @Override
    public Boolean blobExists(String bucketName, String blobKey) {
        HeadObjectRequest request = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(blobKey)
                .build();
        try {
            client.headObject(request);
            return true;
        } catch (RuntimeException error) {
            if (isNotFound(error)) {
                return false;
            }
            throw error;
        }
    }

    @Override
    public String createBlob(String bucketName, Blob blob) {
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
        PutObjectResponse response = client.putObject(request, RequestBody.fromBytes(content));
        return response.eTag();
    }

    @Override
    public Void deleteBlob(String bucketName, String blobKey) {
        DeleteObjectRequest request = DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(blobKey)
                .build();
        client.deleteObject(request);
        return null;
    }

    @Override
    public String copyBlob(String sourceBucketName, String sourceBlobKey, String destinationBucketName, String destinationBlobKey) {
        String copySource = sourceBucketName + "/" + sourceBlobKey;
        CopyObjectRequest request = CopyObjectRequest.builder()
                .copySource(copySource)
                .destinationBucket(destinationBucketName)
                .destinationKey(destinationBlobKey)
                .build();
        CopyObjectResponse response = client.copyObject(request);
        return response.copyObjectResult() == null ? null : response.copyObjectResult().eTag();
    }

    @Override
    public Set<Bucket> listAllBuckets() {
        return mapBuckets(client.listBuckets());
    }

    @Override
    public Set<Blob> listBlobsByPrefix(String bucketName, String prefix) {
        ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                .bucket(bucketName);
        if (prefix != null && !prefix.isBlank()) {
            requestBuilder.prefix(prefix);
        }
        ListObjectsV2Response response = client.listObjectsV2(requestBuilder.build());
        return mapBlobsFromList(bucketName, response);
    }

    @Override
    public Void createBucket(Bucket bucket) {
        CreateBucketRequest request = CreateBucketRequest.builder()
                .bucket(bucket.getName())
                .build();
        client.createBucket(request);
        return null;
    }

    @Override
    public Set<Blob> getAllBlobsInBucket(String bucketName) {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .build();
        return mapBlobsFromList(bucketName, client.listObjectsV2(request));
    }

    @Override
    public Void deleteBucketIfExists(String bucketName) {
        DeleteBucketRequest request = DeleteBucketRequest.builder()
                .bucket(bucketName)
                .build();
        try {
            client.deleteBucket(request);
        } catch (RuntimeException error) {
            if (!isNotFound(error)) {
                throw error;
            }
        }
        return null;
    }

    @Override
    public byte[] getByteRange(String bucketName, String blobKey, long startInclusive, long endInclusive) {
        String range = "bytes=" + startInclusive + "-" + endInclusive;
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(blobKey)
                .range(range)
                .build();
        return client.getObjectAsBytes(request).asByteArray();
    }

    @Override
    public String createBlobIfNotExists(String bucketName, Blob blob) {
        HeadObjectRequest headRequest = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(blob.getKey())
                .build();
        try {
            return client.headObject(headRequest).eTag();
        } catch (RuntimeException error) {
            if (isNotFound(error)) {
                return createBlob(bucketName, blob);
            }
            throw error;
        }
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

    private static boolean isNotFound(Throwable error) {
        if (error instanceof NoSuchBucketException || error instanceof NoSuchKeyException) {
            return true;
        }
        return error instanceof software.amazon.awssdk.services.s3.model.S3Exception s3Exception
                && s3Exception.statusCode() == 404;
    }

    private static void validateExpiry(Duration expiry) {
        if (expiry == null || expiry.isZero() || expiry.isNegative()) {
            throw new IllegalArgumentException("Expiry must be a positive duration.");
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
