package michaelcirkl.ubsa;

import michaelcirkl.ubsa.client.pagination.ListingPage;
import michaelcirkl.ubsa.client.pagination.PageRequest;
import michaelcirkl.ubsa.client.streaming.BlobWriteOptions;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class BlobStorageSyncClientDefaultMethodsTest {
    @Test
    void getBlobMetadataStripsContentWhenUsingDefaultImplementation() {
        Blob fullBlob = Blob.builder()
                .bucket("bucket")
                .key("blob")
                .content("hello".getBytes())
                .size(5L)
                .lastModified(LocalDateTime.of(2025, 1, 2, 3, 4, 5))
                .encoding("gzip")
                .etag("etag-1")
                .userMetadata(java.util.Map.of("k", "v"))
                .publicURI(URI.create("https://example.test/blob"))
                .expires(LocalDateTime.of(2025, 1, 3, 4, 5, 6))
                .build();

        BlobStorageSyncClient client = new StubSyncClient(fullBlob);

        Blob metadata = client.getBlobMetadata("bucket", "blob");

        assertNull(metadata.getContent());
        assertEquals("bucket", metadata.getBucket());
        assertEquals("blob", metadata.getKey());
        assertEquals(5L, metadata.getSize());
        assertEquals("gzip", metadata.encoding());
        assertEquals("etag-1", metadata.getEtag());
        assertEquals(java.util.Map.of("k", "v"), metadata.getUserMetadata());
        assertEquals(URI.create("https://example.test/blob"), metadata.getPublicURI());
        assertEquals(LocalDateTime.of(2025, 1, 2, 3, 4, 5), metadata.lastModified());
        assertEquals(LocalDateTime.of(2025, 1, 3, 4, 5, 6), metadata.expires());
        assertArrayEquals("hello".getBytes(), fullBlob.getContent());
    }

    private static final class StubSyncClient implements BlobStorageSyncClient {
        private final Blob blob;

        private StubSyncClient(Blob blob) {
            this.blob = blob;
        }

        @Override
        public Provider getProvider() {
            return Provider.GCP;
        }

        @Override
        public <T> T unwrap(Class<T> nativeType) {
            return null;
        }

        @Override
        public Boolean bucketExists(String bucketName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Blob getBlob(String bucketName, String blobKey) {
            return blob;
        }

        @Override
        public InputStream openBlobStream(String bucketName, String blobKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Void deleteBucket(String bucketName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Boolean blobExists(String bucketName, String blobKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String createBlob(String bucketName, Blob blob) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String createBlob(String bucketName, String blobKey, Path sourceFile) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String createBlob(String bucketName, String blobKey, InputStream content, long contentLength, BlobWriteOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Void deleteBlobIfExists(String bucketName, String blobKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String copyBlob(String sourceBucketName, String sourceBlobKey, String destinationBucketName, String destinationBlobKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListingPage<Bucket> listBuckets(PageRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListingPage<Blob> listBlobs(String bucketName, String prefix, PageRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Bucket> listAllBuckets() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Void createBucket(Bucket bucket) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Void deleteBucketIfExists(String bucketName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] getByteRange(String bucketName, String blobKey, long startInclusive, long endInclusive) {
            throw new UnsupportedOperationException();
        }

        @Override
        public URL generateGetUrl(String bucket, String objectKey, Duration expiry) {
            throw new UnsupportedOperationException();
        }

        @Override
        public URL generatePutUrl(String bucket, String objectKey, Duration expiry, String contentType) {
            throw new UnsupportedOperationException();
        }
    }
}
