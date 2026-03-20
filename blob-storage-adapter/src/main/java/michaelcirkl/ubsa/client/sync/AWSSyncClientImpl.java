package michaelcirkl.ubsa.client.sync;

import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.*;
import michaelcirkl.ubsa.client.exception.AwsExceptionHandler;
import michaelcirkl.ubsa.client.streaming.BlobWriteOptions;
import michaelcirkl.ubsa.client.streaming.ContentLengthValidators;
import michaelcirkl.ubsa.client.streaming.FileUploadValidators;
import michaelcirkl.ubsa.client.streaming.WriteOptionsMappers;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.Set;

public class AWSSyncClientImpl implements BlobStorageSyncClient {
    private static final String PATH_STYLE_PROBE_BUCKET = "ubsa-path-style-probe";
    private static final String PATH_STYLE_PROBE_KEY = "probe";

    private final AwsExceptionHandler exceptionHandler = new AwsExceptionHandler();
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
        return exceptionHandler.handle(() -> {
            try {
                client.headBucket(request);
                return true;
            } catch (S3Exception error) {
                if (error.statusCode() == 404) {
                    return false;
                }
                throw error;
            }
        });
    }

    @Override
    public Blob getBlob(String bucketName, String blobKey) {
        return exceptionHandler.handle(() -> {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(blobKey)
                    .build();

            ResponseBytes<GetObjectResponse> responseBytes = client.getObjectAsBytes(request);
            return buildBlobFromGetObject(bucketName, blobKey, responseBytes);
        });
    }

    @Override
    public InputStream openBlobStream(String bucketName, String blobKey) {
        return exceptionHandler.handle(() -> {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(blobKey)
                    .build();
            return client.getObject(request);
        });
    }

    @Override
    public Void deleteBucket(String bucketName) {
        return exceptionHandler.handle(() -> {
            DeleteBucketRequest request = DeleteBucketRequest.builder()
                    .bucket(bucketName)
                    .build();
            client.deleteBucket(request);
            return null;
        });
    }

    @Override
    public Boolean blobExists(String bucketName, String blobKey) {
        HeadObjectRequest request = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(blobKey)
                .build();
        return exceptionHandler.handle(() -> {
            try {
                client.headObject(request);
                return true;
            } catch (S3Exception error) {
                if (error.statusCode() == 404) {
                    return false;
                }
                throw error;
            }
        });
    }

    @Override
    public String createBlob(String bucketName, Blob blob) {
        return exceptionHandler.handle(() -> {
            PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(blob.getKey());
            WriteOptionsMappers.applyBlobToAwsPutObject(requestBuilder, blob);

            byte[] content = blob.getContent() == null ? new byte[0] : blob.getContent();
            PutObjectRequest request = requestBuilder.build();
            PutObjectResponse response = client.putObject(request, RequestBody.fromBytes(content));
            return response.eTag();
        });
    }

    @Override
    public String createBlob(String bucketName, String blobKey, Path sourceFile) {
        FileUploadValidators.validateSourceFile(sourceFile);
        return exceptionHandler.handle(() -> {
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(blobKey)
                    .build();
            PutObjectResponse response = client.putObject(request, RequestBody.fromFile(sourceFile));
            return response.eTag();
        });
    }

    @Override
    public String createBlob(String bucketName, String blobKey, InputStream content, long contentLength, BlobWriteOptions options) {
        ContentLengthValidators.validateContentLength(contentLength);
        if (content == null) {
            throw new IllegalArgumentException("Content stream must not be null.");
        }
        validateAwsSinglePutLength(contentLength);
        return exceptionHandler.handle(() -> {
            PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(blobKey);
            WriteOptionsMappers.applyOptionsToAwsPutObject(requestBuilder, options);
            PutObjectResponse response = client.putObject(requestBuilder.build(), RequestBody.fromInputStream(content, contentLength));
            return response.eTag();
        });
    }

    @Override
    public Void deleteBlob(String bucketName, String blobKey) {
        return exceptionHandler.handle(() -> {
            DeleteObjectRequest request = DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(blobKey)
                    .build();
            client.deleteObject(request);
            return null;
        });
    }

    @Override
    public String copyBlob(String sourceBucketName, String sourceBlobKey, String destinationBucketName, String destinationBlobKey) {
        return exceptionHandler.handle(() -> {
            CopyObjectRequest request = CopyObjectRequest.builder()
                    .sourceBucket(sourceBucketName)
                    .sourceKey(sourceBlobKey)
                    .destinationBucket(destinationBucketName)
                    .destinationKey(destinationBlobKey)
                    .build();
            CopyObjectResponse response = client.copyObject(request);
            return response.copyObjectResult() == null ? null : response.copyObjectResult().eTag();
        });
    }

    @Override
    public Set<Bucket> listAllBuckets() {
        return exceptionHandler.handle(() -> mapBuckets(client.listBuckets()));
    }

    @Override
    public Set<Blob> listBlobsByPrefix(String bucketName, String prefix) {
        return exceptionHandler.handle(() -> {
            ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                    .bucket(bucketName);
            if (prefix != null && !prefix.isBlank()) {
                requestBuilder.prefix(prefix);
            }
            return listAllBlobs(bucketName, requestBuilder.build());
        });
    }

    @Override
    public Void createBucket(Bucket bucket) {
        return exceptionHandler.handle(() -> {
            CreateBucketRequest request = CreateBucketRequest.builder()
                    .bucket(bucket.getName())
                    .build();
            client.createBucket(request);
            return null;
        });
    }

    @Override
    public Set<Blob> getAllBlobsInBucket(String bucketName) {
        return exceptionHandler.handle(() -> {
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .build();
            return listAllBlobs(bucketName, request);
        });
    }

    @Override
    public Void deleteBucketIfExists(String bucketName) {
        DeleteBucketRequest request = DeleteBucketRequest.builder()
                .bucket(bucketName)
                .build();

        return exceptionHandler.handle(() -> {
            try {
                client.deleteBucket(request);
                return null;
            } catch (S3Exception error) {
                if (error.statusCode() == 404) {
                    return null;
                }
                throw error;
            }
        });
    }

    @Override
    public byte[] getByteRange(String bucketName, String blobKey, long startInclusive, long endInclusive) {
        validateRange(startInclusive, endInclusive);
        return exceptionHandler.handle(() -> {
            String range = "bytes=" + startInclusive + "-" + endInclusive;
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(blobKey)
                    .range(range)
                    .build();
            return client.getObjectAsBytes(request).asByteArray();
        });
    }

    @Override
    public URL generateGetUrl(String bucket, String objectKey, Duration expiry) {
        return exceptionHandler.handle(() -> {
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
        });
    }

    @Override
    public URL generatePutUrl(String bucket, String objectKey, Duration expiry, String contentType) {
        return exceptionHandler.handle(() -> {
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

    private Set<Blob> listAllBlobs(String bucketName, ListObjectsV2Request initialRequest) {
        Set<Blob> blobs = new HashSet<>();
        ListObjectsV2Request request = initialRequest;

        while (request != null) {
            ListObjectsV2Response response = client.listObjectsV2(request);
            blobs.addAll(mapBlobsFromList(bucketName, response));
            request = response.isTruncated()
                    ? initialRequest.toBuilder().continuationToken(response.nextContinuationToken()).build()
                    : null;
        }

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
    };

    private LocalDateTime toLocalDateTime(Instant instant) {
        return instant == null ? null : LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private URI toS3Uri(String bucketName, String key) {
        String uri = (key == null || key.isBlank())
                ? "s3://" + bucketName
                : "s3://" + bucketName + "/" + key;
        return URI.create(uri);
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
        // PUT object max size is 5 GiB
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
            // If probe fails, it is virtual host style instead of path style
            return false;
        }
    }

    private void validateRange(long startInclusive, long endInclusive) {
        if (startInclusive < 0 || endInclusive < startInclusive) {
            throw new IllegalArgumentException("Invalid range. startInclusive must be >= 0 and endInclusive must be >= startInclusive.");
        }
    }
}
