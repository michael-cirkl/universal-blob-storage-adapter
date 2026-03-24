package org.example.sync;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import michaelcirkl.ubsa.client.exception.UbsaException;
import michaelcirkl.ubsa.client.gcp.GCPSyncClientImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GcpSyncExceptionHandlingTest {
    @Test
    void getBlobMetadataReturnsMetadataWithoutReadingContent() {
        Storage client = mock(Storage.class);
        com.google.cloud.storage.Blob blob = mock(com.google.cloud.storage.Blob.class);
        GCPSyncClientImpl adapter = new GCPSyncClientImpl(client);
        when(client.get("bucket", "blob")).thenReturn(blob);
        when(blob.getSize()).thenReturn(5L);
        when(blob.getUpdateTimeOffsetDateTime()).thenReturn(OffsetDateTime.of(2025, 1, 2, 3, 4, 5, 0, ZoneOffset.UTC));
        when(blob.getContentEncoding()).thenReturn("gzip");
        when(blob.getEtag()).thenReturn("etag-1");
        when(blob.getMetadata()).thenReturn(Map.of("k", "v"));

        michaelcirkl.ubsa.Blob result = adapter.getBlobMetadata("bucket", "blob");

        assertNull(result.getContent());
        assertEquals("bucket", result.getBucket());
        assertEquals("blob", result.getKey());
        assertEquals(5L, result.getSize());
        assertEquals("gzip", result.encoding());
        assertEquals("etag-1", result.getEtag());
        assertEquals(Map.of("k", "v"), result.getUserMetadata());
        assertEquals(URI.create("gs://bucket/blob"), result.getPublicURI());
        assertEquals(LocalDateTime.of(2025, 1, 2, 3, 4, 5), result.lastModified());
        verify(blob, never()).getContent();
    }

    @Test
    void getBlobWrapsStorageFailuresInUbsaException() {
        Storage client = mock(Storage.class);
        GCPSyncClientImpl adapter = new GCPSyncClientImpl(client);
        StorageException failure = new StorageException(500, "gcp failure");
        when(client.get("bucket", "blob")).thenThrow(failure);

        UbsaException error = assertThrows(UbsaException.class, () -> adapter.getBlob("bucket", "blob"));
        assertEquals("gcp failure", error.getMessage());
        assertEquals(500, error.getStatusCode());
        assertSame(failure, error.getCause());
    }

    @Test
    void getBlobTreatsMissingBlobAsNotFound() {
        Storage client = mock(Storage.class);
        GCPSyncClientImpl adapter = new GCPSyncClientImpl(client);
        when(client.get("bucket", "blob")).thenReturn(null);

        UbsaException error = assertThrows(UbsaException.class, () -> adapter.getBlob("bucket", "blob"));
        assertEquals("Blob not found: gs://bucket/blob", error.getMessage());
        assertEquals(404, error.getStatusCode());
        assertInstanceOf(StorageException.class, error.getCause());
    }

    @Test
    void getBlobMetadataWrapsStorageFailuresInUbsaException() {
        Storage client = mock(Storage.class);
        GCPSyncClientImpl adapter = new GCPSyncClientImpl(client);
        StorageException failure = new StorageException(500, "gcp failure");
        when(client.get("bucket", "blob")).thenThrow(failure);

        UbsaException error = assertThrows(UbsaException.class, () -> adapter.getBlobMetadata("bucket", "blob"));
        assertEquals("gcp failure", error.getMessage());
        assertEquals(500, error.getStatusCode());
        assertSame(failure, error.getCause());
    }

    @Test
    void getBlobMetadataTreatsMissingBlobAsNotFound() {
        Storage client = mock(Storage.class);
        GCPSyncClientImpl adapter = new GCPSyncClientImpl(client);
        when(client.get("bucket", "blob")).thenReturn(null);

        UbsaException error = assertThrows(UbsaException.class, () -> adapter.getBlobMetadata("bucket", "blob"));
        assertEquals("Blob not found: gs://bucket/blob", error.getMessage());
        assertEquals(404, error.getStatusCode());
        assertInstanceOf(StorageException.class, error.getCause());
    }

    @Test
    void deleteBucketIfExistsIgnoresMissingBuckets() {
        Storage client = mock(Storage.class);
        GCPSyncClientImpl adapter = new GCPSyncClientImpl(client);
        when(client.delete("bucket")).thenReturn(false);

        assertDoesNotThrow(() -> adapter.deleteBucketIfExists("bucket"));
    }

    @Test
    void deleteBucketIfExistsIgnoresNotFoundFailures() {
        Storage client = mock(Storage.class);
        GCPSyncClientImpl adapter = new GCPSyncClientImpl(client);
        when(client.delete("bucket")).thenThrow(new StorageException(404, "missing"));

        assertDoesNotThrow(() -> adapter.deleteBucketIfExists("bucket"));
    }

    @Test
    void deleteBucketWrapsMissingBucketAsNotFound() {
        Storage client = mock(Storage.class);
        GCPSyncClientImpl adapter = new GCPSyncClientImpl(client);
        when(client.delete("bucket")).thenReturn(false);

        UbsaException error = assertThrows(UbsaException.class, () -> adapter.deleteBucket("bucket"));
        assertEquals("Bucket not found: bucket", error.getMessage());
        assertEquals(404, error.getStatusCode());
        assertInstanceOf(StorageException.class, error.getCause());
    }

    @Test
    void deleteBucketIfExistsWrapsNonNotFoundFailures() {
        Storage client = mock(Storage.class);
        GCPSyncClientImpl adapter = new GCPSyncClientImpl(client);
        StorageException failure = new StorageException(500, "gcp delete failed");
        when(client.delete("bucket")).thenThrow(failure);

        UbsaException error = assertThrows(UbsaException.class, () -> adapter.deleteBucketIfExists("bucket"));
        assertEquals("gcp delete failed", error.getMessage());
        assertEquals(500, error.getStatusCode());
        assertSame(failure, error.getCause());
    }

    @Test
    void createBlobFromFileWrapsIoFailuresInUbsaException(@TempDir Path tempDir) throws IOException {
        Storage client = mock(Storage.class);
        GCPSyncClientImpl adapter = new GCPSyncClientImpl(client);
        Path sourceFile = Files.writeString(tempDir.resolve("blob.txt"), "content");
        IOException failure = new IOException("disk failure");
        when(client.createFrom(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.eq(sourceFile)))
                .thenThrow(failure);

        UbsaException error = assertThrows(UbsaException.class, () -> adapter.createBlob("bucket", "blob", sourceFile));
        assertEquals("disk failure", error.getMessage());
        assertEquals(0, error.getStatusCode());
        assertSame(failure, error.getCause());
    }
}
