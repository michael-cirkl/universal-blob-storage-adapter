package michaelcirkl.ubsa;

import michaelcirkl.ubsa.client.pagination.ListingPage;
import michaelcirkl.ubsa.client.pagination.PagedIterable;
import michaelcirkl.ubsa.client.pagination.PageRequest;
import michaelcirkl.ubsa.client.streaming.BlobWriteOptions;

import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

public interface BlobStorageSyncClient {
    Provider getProvider();

    <T> T unwrap(Class<T> nativeType);

    Boolean bucketExists(String bucketName);

    Blob getBlob(String bucketName, String blobKey);

    /**
     * Returns blob metadata without downloading content bytes. Implementations may return {@code null}
     * from {@link Blob#getContent()} for metadata-only reads.
     */
    default Blob getBlobMetadata(String bucketName, String blobKey) {
        return metadataOnly(getBlob(bucketName, blobKey));
    }

    InputStream openBlobStream(String bucketName, String blobKey);

    Void deleteBucket(String bucketName);

    Boolean blobExists(String bucketName, String blobKey);

    String createBlob(String bucketName, Blob blob);

    String createBlob(String bucketName, String blobKey, Path sourceFile);

    String createBlob(String bucketName, String blobKey, InputStream content, long contentLength, BlobWriteOptions options);

    Void deleteBlobIfExists(String bucketName, String blobKey);

    String copyBlob(
            String sourceBucketName,
            String sourceBlobKey,
            String destinationBucketName,
            String destinationBlobKey
    );

    ListingPage<Bucket> listBuckets(PageRequest request);

    ListingPage<Blob> listBlobs(String bucketName, String prefix, PageRequest request);

    List<Bucket> listAllBuckets();

    default Iterable<Bucket> iterateBuckets(int pageSize) {
        return new PagedIterable<>(PageRequest.builder().pageSize(pageSize).build(), this::listBuckets);
    }

    default Iterable<Blob> iterateBlobs(String bucketName, String prefix, int pageSize) {
        return new PagedIterable<>(
                PageRequest.builder().pageSize(pageSize).build(),
                pageRequest -> listBlobs(bucketName, prefix, pageRequest)
        );
    }

    Void createBucket(Bucket bucket);

    Void deleteBucketIfExists(String bucketName);

    byte[] getByteRange(String bucketName, String blobKey, long startInclusive, long endInclusive);

    URL generateGetUrl(String bucket, String objectKey, Duration expiry);

    URL generatePutUrl(String bucket, String objectKey, Duration expiry, String contentType);

    private static Blob metadataOnly(Blob blob) {
        if (blob == null) {
            return null;
        }
        return Blob.builder()
                .bucket(blob.getBucket())
                .key(blob.getKey())
                .size(blob.getSize())
                .lastModified(blob.lastModified())
                .encoding(blob.encoding())
                .etag(blob.getEtag())
                .userMetadata(blob.getUserMetadata())
                .publicURI(blob.getPublicURI())
                .expires(blob.expires())
                .build();
    }
}
