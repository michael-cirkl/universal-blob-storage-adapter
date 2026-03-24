package michaelcirkl.ubsa;

import michaelcirkl.ubsa.client.pagination.ListingPage;
import michaelcirkl.ubsa.client.pagination.PageRequest;
import michaelcirkl.ubsa.client.streaming.BlobWriteOptions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class BlobStorageAsyncClientDefaultMethodsTest {
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

        BlobStorageAsyncClient client = new StubAsyncClient(fullBlob);

        Blob metadata = client.getBlobMetadata("bucket", "blob").join();

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

    private static final class StubAsyncClient implements BlobStorageAsyncClient {
        private final Blob blob;

        private StubAsyncClient(Blob blob) {
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
        public CompletableFuture<Boolean> bucketExists(String bucketName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Blob> getBlob(String bucketName, String blobKey) {
            return CompletableFuture.completedFuture(blob);
        }

        @Override
        public Flow.Publisher<ByteBuffer> openBlobStream(String bucketName, String blobKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> deleteBucket(String bucketName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Boolean> blobExists(String bucketName, String blobKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<String> createBlob(String bucketName, Blob blob) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<String> createBlob(String bucketName, String blobKey, Path sourceFile) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<String> createBlob(String bucketName, String blobKey, Flow.Publisher<ByteBuffer> content, long contentLength, BlobWriteOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> deleteBlobIfExists(String bucketName, String blobKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<String> copyBlob(String sourceBucketName, String sourceBlobKey, String destinationBucketName, String destinationBlobKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<ListingPage<Bucket>> listBuckets(PageRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<ListingPage<Blob>> listBlobs(String bucketName, String prefix, PageRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<List<Bucket>> listAllBuckets() {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> createBucket(Bucket bucket) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> deleteBucketIfExists(String bucketName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<byte[]> getByteRange(String bucketName, String blobKey, long startInclusive, long endInclusive) {
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
