package michaelcirkl.ubsa.client.gcp;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.HttpMethod;
import com.google.cloud.storage.Storage;
import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.client.pagination.PageRequest;

import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class GCPClientSupport {
    private GCPClientSupport() {
    }

    public static List<Bucket> mapBuckets(Iterable<com.google.cloud.storage.Bucket> bucketItems) {
        List<Bucket> buckets = new ArrayList<>();
        bucketItems.forEach(gcsBucket -> {
            LocalDateTime created = toLocalDateTime(gcsBucket.getCreateTimeOffsetDateTime());
            LocalDateTime updated = toLocalDateTime(gcsBucket.getUpdateTimeOffsetDateTime());
            buckets.add(Bucket.builder()
                    .name(gcsBucket.getName())
                    .publicURI(toGsUri(gcsBucket.getName(), null))
                    .creationDate(created)
                    .lastModified(updated)
                    .build());
        });
        return buckets;
    }

    public static List<Blob> mapBlobsFromPage(String bucketName, Iterable<com.google.cloud.storage.Blob> blobItems) {
        List<Blob> blobs = new ArrayList<>();
        blobItems.forEach(gcsBlob -> blobs.add(mapBlobSummary(bucketName, gcsBlob)));
        return blobs;
    }

    public static Blob mapFetchedBlob(String bucketName, String blobKey, BlobInfo blobInfo, byte[] content) {
        return Blob.builder()
                .content(content)
                .bucket(bucketName)
                .key(blobKey)
                .size(blobInfo.getSize() == null ? 0L : blobInfo.getSize())
                .lastModified(toLocalDateTime(blobInfo.getUpdateTimeOffsetDateTime()))
                .encoding(blobInfo.getContentEncoding())
                .etag(blobInfo.getEtag())
                .userMetadata(blobInfo.getMetadata())
                .publicURI(toGsUri(bucketName, blobKey))
                .build();
    }

    public static Blob mapBlobMetadata(String bucketName, String blobKey, BlobInfo blobInfo) {
        return Blob.builder()
                .bucket(bucketName)
                .key(blobKey)
                .size(blobInfo.getSize() == null ? 0L : blobInfo.getSize())
                .lastModified(toLocalDateTime(blobInfo.getUpdateTimeOffsetDateTime()))
                .encoding(blobInfo.getContentEncoding())
                .etag(blobInfo.getEtag())
                .userMetadata(blobInfo.getMetadata())
                .publicURI(toGsUri(bucketName, blobKey))
                .build();
    }

    public static Storage.BucketListOption[] buildBucketListOptions(PageRequest request) {
        List<Storage.BucketListOption> options = new ArrayList<>();
        if (request.getPageSize() != null) {
            options.add(Storage.BucketListOption.pageSize(request.getPageSize()));
        }
        if (request.getContinuationToken() != null) {
            options.add(Storage.BucketListOption.pageToken(request.getContinuationToken()));
        }
        return options.toArray(Storage.BucketListOption[]::new);
    }

    public static Storage.BlobListOption[] buildBlobListOptions(String prefix, PageRequest request) {
        List<Storage.BlobListOption> options = new ArrayList<>();
        if (prefix != null && !prefix.isBlank()) {
            options.add(Storage.BlobListOption.prefix(prefix));
        }
        if (request.getPageSize() != null) {
            options.add(Storage.BlobListOption.pageSize(request.getPageSize()));
        }
        if (request.getContinuationToken() != null) {
            options.add(Storage.BlobListOption.pageToken(request.getContinuationToken()));
        }
        return options.toArray(Storage.BlobListOption[]::new);
    }

    public static PageRequest normalizePageRequest(PageRequest request) {
        return request == null ? PageRequest.firstPage() : request;
    }

    public static long toPositiveSeconds(Duration expiry) {
        if (expiry == null || expiry.isZero() || expiry.isNegative()) {
            throw new IllegalArgumentException("Expiry must be a positive duration.");
        }
        return expiry.toSeconds();
    }

    public static URL generateGetUrl(Storage client, String bucket, String objectKey, Duration expiry) {
        long seconds = toPositiveSeconds(expiry);
        BlobInfo blobInfo = BlobInfo.newBuilder(bucket, objectKey).build();
        return client.signUrl(blobInfo, seconds, TimeUnit.SECONDS);
    }

    public static URL generatePutUrl(Storage client, String bucket, String objectKey, Duration expiry, String contentType) {
        long seconds = toPositiveSeconds(expiry);
        BlobInfo.Builder blobBuilder = BlobInfo.newBuilder(bucket, objectKey);
        if (contentType != null && !contentType.isBlank()) {
            blobBuilder.setContentType(contentType);
        }
        BlobInfo blobInfo = blobBuilder.build();
        List<Storage.SignUrlOption> options = new ArrayList<>();
        options.add(Storage.SignUrlOption.httpMethod(HttpMethod.PUT));
        if (contentType != null && !contentType.isBlank()) {
            options.add(Storage.SignUrlOption.withContentType());
        }
        return client.signUrl(blobInfo, seconds, TimeUnit.SECONDS, options.toArray(Storage.SignUrlOption[]::new));
    }

    public static URI toGsUri(String bucketName, String objectKey) {
        String uri = (objectKey == null || objectKey.isBlank())
                ? "gs://" + bucketName
                : "gs://" + bucketName + "/" + objectKey;
        return URI.create(uri);
    }

    private static Blob mapBlobSummary(String bucketName, BlobInfo blobInfo) {
        return mapBlobMetadata(bucketName, blobInfo.getName(), blobInfo);
    }

    private static LocalDateTime toLocalDateTime(OffsetDateTime time) {
        return time == null ? null : time.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime();
    }

    public static void validateExpiry(Duration expiry) {
        if (expiry == null || expiry.isZero() || expiry.isNegative()) {
            throw new IllegalArgumentException("Expiry must be a positive duration.");
        }
    }
}
