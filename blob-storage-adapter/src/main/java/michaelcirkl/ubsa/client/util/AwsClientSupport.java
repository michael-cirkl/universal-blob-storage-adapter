package michaelcirkl.ubsa.client.util;

import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.Bucket;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.GetUrlRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public final class AwsClientSupport {
    private static final String PATH_STYLE_PROBE_BUCKET = "ubsa-path-style-probe";
    private static final String PATH_STYLE_PROBE_KEY = "probe";

    private AwsClientSupport() {
    }

    public static List<Bucket> mapBuckets(ListBucketsResponse response) {
        List<Bucket> buckets = new ArrayList<>();
        response.buckets().forEach(bucket -> {
            LocalDateTime creation = toLocalDateTime(bucket.creationDate());
            buckets.add(Bucket.builder()
                    .name(bucket.name())
                    .publicURI(toS3Uri(bucket.name(), null))
                    .creationDate(creation)
                    .lastModified(null)
                    .build());
        });
        return buckets;
    }

    public static List<Blob> mapBlobsFromList(String bucketName, ListObjectsV2Response response) {
        List<Blob> blobs = new ArrayList<>();
        response.contents().forEach(object -> blobs.add(Blob.builder()
                .bucket(bucketName)
                .key(object.key())
                .size(object.size())
                .lastModified(toLocalDateTime(object.lastModified()))
                .etag(object.eTag())
                .publicURI(toS3Uri(bucketName, object.key()))
                .build()));
        return blobs;
    }

    public static Blob buildBlobFromGetObject(String bucketName, String blobKey, ResponseBytes<GetObjectResponse> responseBytes) {
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

    public static void validateExpiry(Duration expiry) {
        if (expiry == null || expiry.isZero() || expiry.isNegative()) {
            throw new IllegalArgumentException("Expiry must be a positive duration.");
        }
    }

    public static URL presignGetUrl(String bucket, String objectKey, Duration expiry, Supplier<S3Presigner> presignerSupplier) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucket)
                .key(objectKey)
                .build();
        GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                .signatureDuration(expiry)
                .getObjectRequest(getObjectRequest)
                .build();
        try (S3Presigner presigner = presignerSupplier.get()) {
            return presigner.presignGetObject(presignRequest).url();
        }
    }

    public static URL presignPutUrl(
            String bucket,
            String objectKey,
            Duration expiry,
            String contentType,
            Supplier<S3Presigner> presignerSupplier
    ) {
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
        try (S3Presigner presigner = presignerSupplier.get()) {
            return presigner.presignPutObject(presignRequest).url();
        }
    }

    public static S3Presigner createPresignerFromClientConfig(
            S3ServiceClientConfiguration config,
            Supplier<URL> probeUrlSupplier
    ) {
        S3Presigner.Builder builder = S3Presigner.builder()
                .region(config.region())
                .credentialsProvider(config.credentialsProvider())
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(isPathStyleEnabled(probeUrlSupplier))
                        .build());
        config.endpointOverride().ifPresent(builder::endpointOverride);
        return builder.build();
    }

    public static GetUrlRequest pathStyleProbeRequest() {
        return GetUrlRequest.builder()
                .bucket(PATH_STYLE_PROBE_BUCKET)
                .key(PATH_STYLE_PROBE_KEY)
                .build();
    }

    public static boolean isPathStyleEnabled(Supplier<URL> probeUrlSupplier) {
        try {
            URL probeUrl = probeUrlSupplier.get();
            String expectedPrefix = "/" + PATH_STYLE_PROBE_BUCKET + "/";
            return probeUrl.getPath() != null && probeUrl.getPath().startsWith(expectedPrefix);
        } catch (RuntimeException ignored) {
            return false;
        }
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

    private static LocalDateTime parseExpiresHeader(String expiresHeader) {
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
}
