package michaelcirkl.ubsa.client.sync;

import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.*;
import michaelcirkl.ubsa.client.pagination.ListingPage;
import michaelcirkl.ubsa.client.pagination.PageRequest;
import michaelcirkl.ubsa.client.util.AwsClientSupport;
import michaelcirkl.ubsa.client.exception.AWSExceptionHandler;
import michaelcirkl.ubsa.client.streaming.BlobWriteOptions;
import michaelcirkl.ubsa.client.streaming.ContentLengthValidators;
import michaelcirkl.ubsa.client.streaming.FileUploadValidators;
import michaelcirkl.ubsa.client.streaming.WriteOptionsMappers;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.time.Duration;

public class AWSSyncClientImpl implements BlobStorageSyncClient {
    private final AWSExceptionHandler exceptionHandler = new AWSExceptionHandler();
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
            return AwsClientSupport.buildBlobFromGetObject(bucketName, blobKey, responseBytes);
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
    public Void deleteBlobIfExists(String bucketName, String blobKey) {
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
    public ListingPage<Bucket> listBuckets(PageRequest request) {
        PageRequest pageRequest = normalizePageRequest(request);
        return exceptionHandler.handle(() -> {
            ListBucketsRequest.Builder requestBuilder = ListBucketsRequest.builder();
            if (pageRequest.getPageSize() != null) {
                requestBuilder.maxBuckets(pageRequest.getPageSize());
            }
            if (pageRequest.getContinuationToken() != null) {
                requestBuilder.continuationToken(pageRequest.getContinuationToken());
            }

            ListBucketsResponse response = client.listBuckets(requestBuilder.build());
            return ListingPage.of(AwsClientSupport.mapBuckets(response), response.continuationToken());
        });
    }

    @Override
    public ListingPage<Blob> listBlobs(String bucketName, String prefix, PageRequest request) {
        PageRequest pageRequest = normalizePageRequest(request);
        return exceptionHandler.handle(() -> {
            ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                    .bucket(bucketName);
            if (prefix != null && !prefix.isBlank()) {
                requestBuilder.prefix(prefix);
            }
            if (pageRequest.getPageSize() != null) {
                requestBuilder.maxKeys(pageRequest.getPageSize());
            }
            if (pageRequest.getContinuationToken() != null) {
                requestBuilder.continuationToken(pageRequest.getContinuationToken());
            }

            ListObjectsV2Response response = client.listObjectsV2(requestBuilder.build());
            return ListingPage.of(AwsClientSupport.mapBlobsFromList(bucketName, response), response.nextContinuationToken());
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
            AwsClientSupport.validateExpiry(expiry);
            return AwsClientSupport.presignGetUrl(bucket, objectKey, expiry, this::createPresignerFromClientConfig);
        });
    }

    @Override
    public URL generatePutUrl(String bucket, String objectKey, Duration expiry, String contentType) {
        return exceptionHandler.handle(() -> {
            AwsClientSupport.validateExpiry(expiry);
            return AwsClientSupport.presignPutUrl(bucket, objectKey, expiry, contentType, this::createPresignerFromClientConfig);
        });
    }

    private void validateAwsSinglePutLength(long contentLength) {
        // PUT object max size is 5 GiB
        long maxSinglePutBytes = 5L * 1024L * 1024L * 1024L;
        if (contentLength > maxSinglePutBytes) {
            throw new IllegalArgumentException("AWS single PUT upload supports up to 5 GiB. Received: " + contentLength + " bytes.");
        }
    }

    private software.amazon.awssdk.services.s3.presigner.S3Presigner createPresignerFromClientConfig() {
        return AwsClientSupport.createPresignerFromClientConfig(
                client.serviceClientConfiguration(),
                () -> client.utilities().getUrl(AwsClientSupport.pathStyleProbeRequest())
        );
    }

    private void validateRange(long startInclusive, long endInclusive) {
        if (startInclusive < 0 || endInclusive < startInclusive) {
            throw new IllegalArgumentException("Invalid range. startInclusive must be >= 0 and endInclusive must be >= startInclusive.");
        }
    }

    private PageRequest normalizePageRequest(PageRequest request) {
        return request == null ? PageRequest.firstPage() : request;
    }
}
