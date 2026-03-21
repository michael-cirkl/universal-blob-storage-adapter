package org.example.sync;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import michaelcirkl.ubsa.client.exception.UbsaException;
import michaelcirkl.ubsa.client.sync.GCPSyncClientImpl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GcpSyncExceptionHandlingTest {
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
}
