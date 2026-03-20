package org.example.sync;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobContainerItemProperties;
import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.client.sync.AzureSyncClientImpl;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AzureSyncListingTest {
    @Test
    void listAllBucketsLeavesCreationDateUnknown() {
        BlobServiceClient serviceClient = mock(BlobServiceClient.class);
        @SuppressWarnings("unchecked")
        PagedIterable<BlobContainerItem> containers = mock(PagedIterable.class);
        BlobContainerItem containerItem = mock(BlobContainerItem.class);
        BlobContainerItemProperties properties = mock(BlobContainerItemProperties.class);
        BlobContainerClient containerClient = mock(BlobContainerClient.class);
        OffsetDateTime lastModified = OffsetDateTime.of(2025, 1, 2, 3, 4, 5, 0, ZoneOffset.UTC);

        when(serviceClient.listBlobContainers()).thenReturn(containers);
        doAnswer(invocation -> {
            Consumer<BlobContainerItem> consumer = invocation.getArgument(0);
            consumer.accept(containerItem);
            return null;
        }).when(containers).forEach(any());
        when(containerItem.getName()).thenReturn("bucket");
        when(containerItem.getProperties()).thenReturn(properties);
        when(properties.getLastModified()).thenReturn(lastModified);
        when(serviceClient.getBlobContainerClient("bucket")).thenReturn(containerClient);
        when(containerClient.getBlobContainerUrl()).thenReturn("https://example.test/bucket");

        AzureSyncClientImpl adapter = new AzureSyncClientImpl(serviceClient);
        Set<Bucket> buckets = adapter.listAllBuckets();

        assertEquals(1, buckets.size());
        Bucket bucket = buckets.iterator().next();
        assertEquals("bucket", bucket.getName());
        assertNull(bucket.getCreationDate());
        assertEquals(lastModified.toLocalDateTime(), bucket.getLastModified());
    }
}
