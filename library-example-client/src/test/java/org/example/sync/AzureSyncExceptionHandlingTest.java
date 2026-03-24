package org.example.sync;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobStorageException;
import michaelcirkl.ubsa.client.exception.UbsaException;
import michaelcirkl.ubsa.client.azure.AzureSyncClientImpl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AzureSyncExceptionHandlingTest {
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
