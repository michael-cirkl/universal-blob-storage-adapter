package org.example.sync;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Storage;
import michaelcirkl.ubsa.client.pagination.ListingPage;
import michaelcirkl.ubsa.client.pagination.PageRequest;
import michaelcirkl.ubsa.client.sync.GCPSyncClientImpl;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GcpSyncListingTest {
    @Test
    void listBucketsUsesCreateAndUpdateTimesSeparately() {
        Storage client = mock(Storage.class);
        @SuppressWarnings("unchecked")
        Page<com.google.cloud.storage.Bucket> page = mock(Page.class);
        com.google.cloud.storage.Bucket gcsBucket = mock(com.google.cloud.storage.Bucket.class);
        OffsetDateTime created = OffsetDateTime.of(2025, 1, 2, 3, 4, 5, 0, ZoneOffset.UTC);
        OffsetDateTime updated = OffsetDateTime.of(2025, 2, 3, 4, 5, 6, 0, ZoneOffset.UTC);

        when(client.list(any(Storage.BucketListOption[].class))).thenReturn(page);
        when(page.getValues()).thenReturn(List.of(gcsBucket));
        when(page.getNextPageToken()).thenReturn("page-2");
        when(gcsBucket.getName()).thenReturn("bucket");
        when(gcsBucket.getCreateTimeOffsetDateTime()).thenReturn(created);
        when(gcsBucket.getUpdateTimeOffsetDateTime()).thenReturn(updated);

        GCPSyncClientImpl adapter = new GCPSyncClientImpl(client);
        ListingPage<michaelcirkl.ubsa.Bucket> buckets = adapter.listBuckets(PageRequest.builder().pageSize(5).build());

        assertEquals(1, buckets.getItems().size());
        michaelcirkl.ubsa.Bucket bucket = buckets.getItems().get(0);
        assertEquals("bucket", bucket.getName());
        assertEquals(created.toLocalDateTime(), bucket.getCreationDate());
        assertEquals(updated.toLocalDateTime(), bucket.getLastModified());
        assertTrue(buckets.hasNextPage());
        assertEquals("page-2", buckets.getNextContinuationToken());
    }
}
