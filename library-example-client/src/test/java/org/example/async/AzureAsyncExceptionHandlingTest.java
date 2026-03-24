package org.example.async;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.client.azure.AzureAsyncClientImpl;
import michaelcirkl.ubsa.client.exception.UbsaException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AzureAsyncExceptionHandlingTest {
    @Test
    void getBlobMetadataReadsPropertiesOnly() {
        BlobServiceAsyncClient serviceClient = mock(BlobServiceAsyncClient.class);
        BlobContainerAsyncClient containerClient = mock(BlobContainerAsyncClient.class);
        BlobAsyncClient blobClient = mock(BlobAsyncClient.class);
        BlobProperties properties = mock(BlobProperties.class);

        when(serviceClient.getBlobContainerAsyncClient("bucket")).thenReturn(containerClient);
        when(containerClient.getBlobAsyncClient("blob")).thenReturn(blobClient);
        when(blobClient.getProperties()).thenReturn(Mono.just(properties));
        when(blobClient.getBlobUrl()).thenReturn("https://example.test/bucket/blob");
        when(properties.getBlobSize()).thenReturn(5L);
        when(properties.getLastModified()).thenReturn(OffsetDateTime.of(2025, 1, 2, 3, 4, 5, 0, ZoneOffset.UTC));
        when(properties.getContentEncoding()).thenReturn("gzip");
        when(properties.getETag()).thenReturn("etag-1");
        when(properties.getMetadata()).thenReturn(Map.of("k", "v"));
        when(properties.getExpiresOn()).thenReturn(OffsetDateTime.of(2025, 1, 2, 4, 5, 6, 0, ZoneOffset.UTC));

        AzureAsyncClientImpl adapter = new AzureAsyncClientImpl(serviceClient);
        Blob blob = adapter.getBlobMetadata("bucket", "blob").join();

        assertNull(blob.getContent());
        assertEquals("bucket", blob.getBucket());
        assertEquals("blob", blob.getKey());
        assertEquals(5L, blob.getSize());
        assertEquals("gzip", blob.encoding());
        assertEquals("etag-1", blob.getEtag());
        assertEquals(Map.of("k", "v"), blob.getUserMetadata());
        assertEquals(URI.create("https://example.test/bucket/blob"), blob.getPublicURI());
        assertEquals(LocalDateTime.of(2025, 1, 2, 3, 4, 5), blob.lastModified());
        assertEquals(LocalDateTime.of(2025, 1, 2, 4, 5, 6), blob.expires());
        verify(blobClient, never()).downloadContent();
    }

    @Test
    void getBlobWrapsAzureFailuresInUbsaException() {
        BlobServiceAsyncClient serviceClient = mock(BlobServiceAsyncClient.class);
        BlobContainerAsyncClient containerClient = mock(BlobContainerAsyncClient.class);
        BlobAsyncClient blobClient = mock(BlobAsyncClient.class);
        BlobStorageException failure = blobStorageException(500, "azure failure");

        when(serviceClient.getBlobContainerAsyncClient("bucket")).thenReturn(containerClient);
        when(containerClient.getBlobAsyncClient("blob")).thenReturn(blobClient);
        when(blobClient.getProperties()).thenReturn(Mono.error(failure));
        when(blobClient.downloadContent()).thenReturn(Mono.just(BinaryData.fromBytes(new byte[0])));

        AzureAsyncClientImpl adapter = new AzureAsyncClientImpl(serviceClient);
        CompletionException completionError = assertThrows(CompletionException.class, () -> adapter.getBlob("bucket", "blob").join());

        assertTrue(completionError.getCause() instanceof UbsaException);
        UbsaException error = (UbsaException) completionError.getCause();
        assertEquals("azure failure", error.getMessage());
        assertEquals(500, error.getStatusCode());
        assertSame(failure, error.getCause());
    }

    @Test
    void getBlobMetadataWrapsAzureFailuresInUbsaException() {
        BlobServiceAsyncClient serviceClient = mock(BlobServiceAsyncClient.class);
        BlobContainerAsyncClient containerClient = mock(BlobContainerAsyncClient.class);
        BlobAsyncClient blobClient = mock(BlobAsyncClient.class);
        BlobStorageException failure = blobStorageException(500, "azure failure");

        when(serviceClient.getBlobContainerAsyncClient("bucket")).thenReturn(containerClient);
        when(containerClient.getBlobAsyncClient("blob")).thenReturn(blobClient);
        when(blobClient.getProperties()).thenReturn(Mono.error(failure));

        AzureAsyncClientImpl adapter = new AzureAsyncClientImpl(serviceClient);
        CompletionException completionError = assertThrows(CompletionException.class, () -> adapter.getBlobMetadata("bucket", "blob").join());

        assertTrue(completionError.getCause() instanceof UbsaException);
        UbsaException error = (UbsaException) completionError.getCause();
        assertEquals("azure failure", error.getMessage());
        assertEquals(500, error.getStatusCode());
        assertSame(failure, error.getCause());
    }

    @Test
    void deleteBucketIfExistsWrapsNonNotFoundFailures() {
        BlobServiceAsyncClient serviceClient = mock(BlobServiceAsyncClient.class);
        BlobContainerAsyncClient containerClient = mock(BlobContainerAsyncClient.class);
        BlobStorageException failure = blobStorageException(500, "azure delete failed");

        when(serviceClient.getBlobContainerAsyncClient("bucket")).thenReturn(containerClient);
        when(containerClient.deleteIfExists()).thenReturn(Mono.error(failure));

        AzureAsyncClientImpl adapter = new AzureAsyncClientImpl(serviceClient);
        CompletionException completionError = assertThrows(CompletionException.class, () -> adapter.deleteBucketIfExists("bucket").join());

        assertTrue(completionError.getCause() instanceof UbsaException);
        UbsaException error = (UbsaException) completionError.getCause();
        assertEquals("azure delete failed", error.getMessage());
        assertEquals(500, error.getStatusCode());
        assertSame(failure, error.getCause());
    }

    @Test
    void generatePutUrlWrapsAzureFailuresInUbsaException() {
        BlobServiceAsyncClient serviceClient = mock(BlobServiceAsyncClient.class);
        BlobContainerAsyncClient containerClient = mock(BlobContainerAsyncClient.class);
        BlobAsyncClient blobClient = mock(BlobAsyncClient.class);
        BlobStorageException failure = blobStorageException(503, "azure sas failed");

        when(serviceClient.getBlobContainerAsyncClient("bucket")).thenReturn(containerClient);
        when(containerClient.getBlobAsyncClient("blob")).thenReturn(blobClient);
        when(blobClient.generateSas(any(BlobServiceSasSignatureValues.class))).thenThrow(failure);

        AzureAsyncClientImpl adapter = new AzureAsyncClientImpl(serviceClient);
        UbsaException error = assertThrows(
                UbsaException.class,
                () -> adapter.generatePutUrl("bucket", "blob", Duration.ofMinutes(1), "text/plain")
        );
        assertEquals("azure sas failed", error.getMessage());
        assertEquals(503, error.getStatusCode());
        assertSame(failure, error.getCause());
    }

    @Test
    void openBlobStreamWrapsAzureFailuresInUbsaException() throws Exception {
        BlobServiceAsyncClient serviceClient = mock(BlobServiceAsyncClient.class);
        BlobContainerAsyncClient containerClient = mock(BlobContainerAsyncClient.class);
        BlobAsyncClient blobClient = mock(BlobAsyncClient.class);
        BlobStorageException failure = blobStorageException(404, "azure missing");

        when(serviceClient.getBlobContainerAsyncClient("bucket")).thenReturn(containerClient);
        when(containerClient.getBlobAsyncClient("blob")).thenReturn(blobClient);
        when(blobClient.downloadStream()).thenReturn(Flux.error(failure));

        AzureAsyncClientImpl adapter = new AzureAsyncClientImpl(serviceClient);
        Throwable error = streamError(adapter.openBlobStream("bucket", "blob"));

        assertTrue(error instanceof UbsaException);
        UbsaException ubsaError = (UbsaException) error;
        assertEquals("azure missing", ubsaError.getMessage());
        assertEquals(404, ubsaError.getStatusCode());
        assertSame(failure, ubsaError.getCause());
    }

    private static BlobStorageException blobStorageException(int statusCode, String message) {
        BlobStorageException error = mock(BlobStorageException.class);
        when(error.getStatusCode()).thenReturn(statusCode);
        when(error.getMessage()).thenReturn(message);
        return error;
    }

    private static Throwable streamError(Flow.Publisher<ByteBuffer> publisher) throws Exception {
        CompletableFuture<Throwable> future = new CompletableFuture<>();
        publisher.subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ByteBuffer item) {
            }

            @Override
            public void onError(Throwable throwable) {
                future.complete(throwable);
            }

            @Override
            public void onComplete() {
                future.completeExceptionally(new AssertionError("Expected stream failure"));
            }
        });
        return future.get(5, TimeUnit.SECONDS);
    }
}
