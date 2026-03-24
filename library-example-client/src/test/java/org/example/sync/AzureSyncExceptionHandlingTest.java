package org.example.sync;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import michaelcirkl.ubsa.client.exception.UbsaException;
import michaelcirkl.ubsa.client.azure.AzureSyncClientImpl;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AzureSyncExceptionHandlingTest {
    @Test
    void getBlobMetadataReadsPropertiesOnly() {
        BlobServiceClient serviceClient = mock(BlobServiceClient.class);
        BlobContainerClient containerClient = mock(BlobContainerClient.class);
        BlobClient blobClient = mock(BlobClient.class);
        BlobProperties properties = mock(BlobProperties.class);

        when(serviceClient.getBlobContainerClient("bucket")).thenReturn(containerClient);
        when(containerClient.getBlobClient("blob")).thenReturn(blobClient);
        when(blobClient.getProperties()).thenReturn(properties);
        when(blobClient.getBlobUrl()).thenReturn("https://example.test/bucket/blob");
        when(properties.getBlobSize()).thenReturn(5L);
        when(properties.getLastModified()).thenReturn(OffsetDateTime.of(2025, 1, 2, 3, 4, 5, 0, ZoneOffset.UTC));
        when(properties.getContentEncoding()).thenReturn("gzip");
        when(properties.getETag()).thenReturn("etag-1");
        when(properties.getMetadata()).thenReturn(Map.of("k", "v"));
        when(properties.getExpiresOn()).thenReturn(OffsetDateTime.of(2025, 1, 2, 4, 5, 6, 0, ZoneOffset.UTC));

        AzureSyncClientImpl adapter = new AzureSyncClientImpl(serviceClient);
        michaelcirkl.ubsa.Blob blob = adapter.getBlobMetadata("bucket", "blob");

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
        BlobServiceClient serviceClient = mock(BlobServiceClient.class);
        BlobContainerClient containerClient = mock(BlobContainerClient.class);
        BlobClient blobClient = mock(BlobClient.class);
        BlobStorageException failure = blobStorageException(500, "azure failure");

        when(serviceClient.getBlobContainerClient("bucket")).thenReturn(containerClient);
        when(containerClient.getBlobClient("blob")).thenReturn(blobClient);
        when(blobClient.getProperties()).thenThrow(failure);

        AzureSyncClientImpl adapter = new AzureSyncClientImpl(serviceClient);
        UbsaException error = assertThrows(UbsaException.class, () -> adapter.getBlob("bucket", "blob"));
        assertEquals("azure failure", error.getMessage());
        assertEquals(500, error.getStatusCode());
        assertSame(failure, error.getCause());
    }

    @Test
    void getBlobMetadataWrapsAzureFailuresInUbsaException() {
        BlobServiceClient serviceClient = mock(BlobServiceClient.class);
        BlobContainerClient containerClient = mock(BlobContainerClient.class);
        BlobClient blobClient = mock(BlobClient.class);
        BlobStorageException failure = blobStorageException(500, "azure failure");

        when(serviceClient.getBlobContainerClient("bucket")).thenReturn(containerClient);
        when(containerClient.getBlobClient("blob")).thenReturn(blobClient);
        when(blobClient.getProperties()).thenThrow(failure);

        AzureSyncClientImpl adapter = new AzureSyncClientImpl(serviceClient);
        UbsaException error = assertThrows(UbsaException.class, () -> adapter.getBlobMetadata("bucket", "blob"));
        assertEquals("azure failure", error.getMessage());
        assertEquals(500, error.getStatusCode());
        assertSame(failure, error.getCause());
    }

    @Test
    void deleteBucketIfExistsWrapsNonNotFoundFailures() {
        BlobServiceClient serviceClient = mock(BlobServiceClient.class);
        BlobContainerClient containerClient = mock(BlobContainerClient.class);
        BlobStorageException failure = blobStorageException(500, "azure delete failed");

        when(serviceClient.getBlobContainerClient("bucket")).thenReturn(containerClient);
        when(containerClient.deleteIfExists()).thenThrow(failure);

        AzureSyncClientImpl adapter = new AzureSyncClientImpl(serviceClient);
        UbsaException error = assertThrows(UbsaException.class, () -> adapter.deleteBucketIfExists("bucket"));
        assertEquals("azure delete failed", error.getMessage());
        assertEquals(500, error.getStatusCode());
        assertSame(failure, error.getCause());
    }

    private static BlobStorageException blobStorageException(int statusCode, String message) {
        BlobStorageException error = mock(BlobStorageException.class);
        when(error.getStatusCode()).thenReturn(statusCode);
        when(error.getMessage()).thenReturn(message);
        return error;
    }
}
