package org.example.async;

import com.google.api.core.ApiFutures;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import michaelcirkl.ubsa.client.async.GCPAsyncClientImpl;
import michaelcirkl.ubsa.client.exception.UbsaException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GcpAsyncExceptionHandlingTest {
    @Test
    void getBlobWrapsStorageFailuresInUbsaException() {
        Storage client = mock(Storage.class);
        GCPAsyncClientImpl adapter = new GCPAsyncClientImpl(client);
        StorageException failure = new StorageException(500, "gcp failure");
        when(client.blobReadSession(any(BlobId.class))).thenReturn(ApiFutures.immediateFailedFuture(failure));

        CompletionException completionError = assertThrows(CompletionException.class, () -> adapter.getBlob("bucket", "blob").join());

        assertTrue(completionError.getCause() instanceof UbsaException);
        UbsaException error = (UbsaException) completionError.getCause();
        assertEquals("gcp failure", error.getMessage());
        assertEquals(500, error.getStatusCode());
        assertSame(failure, error.getCause());
    }

    @Test
    void deleteBucketIfExistsIgnoresNotFoundFailures() {
        Storage client = mock(Storage.class);
        GCPAsyncClientImpl adapter = new GCPAsyncClientImpl(client);
        when(client.delete("bucket")).thenThrow(new StorageException(404, "missing"));

        assertDoesNotThrow(() -> adapter.deleteBucketIfExists("bucket").join());
    }

    @Test
    void generateGetUrlWrapsStorageFailuresInUbsaException() {
        Storage client = mock(Storage.class);
        GCPAsyncClientImpl adapter = new GCPAsyncClientImpl(client);
        StorageException failure = new StorageException(503, "sign failed");
        when(client.signUrl(any(BlobInfo.class), eq(60L), eq(TimeUnit.SECONDS))).thenThrow(failure);

        UbsaException error = assertThrows(
                UbsaException.class,
                () -> adapter.generateGetUrl("bucket", "blob", Duration.ofMinutes(1))
        );
        assertEquals("sign failed", error.getMessage());
        assertEquals(503, error.getStatusCode());
        assertSame(failure, error.getCause());
    }

    @Test
    void createBlobFromFilePreservesHelperUbsaException(@TempDir Path tempDir) throws IOException {
        Storage client = mock(Storage.class);
        GCPAsyncClientImpl adapter = new GCPAsyncClientImpl(client);
        Path sourceFile = Files.writeString(tempDir.resolve("blob.txt"), "content");
        IOException failure = new IOException("disk failure");
        when(client.createFrom(any(BlobInfo.class), eq(sourceFile))).thenThrow(failure);

        CompletionException completionError = assertThrows(
                CompletionException.class,
                () -> adapter.createBlob("bucket", "blob", sourceFile).join()
        );

        assertTrue(completionError.getCause() instanceof UbsaException);
        UbsaException error = (UbsaException) completionError.getCause();
        assertEquals("Failed to create GCP blob gs://bucket/blob from file", error.getMessage());
        assertEquals(0, error.getStatusCode());
        assertInstanceOf(IOException.class, error.getCause());
        assertSame(failure, error.getCause());
    }
}
