package michaelcirkl.ubsa.client.pagination;

import michaelcirkl.ubsa.Bucket;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AsyncBucketListingSupportTest {
    @Test
    void listAllBucketsTraversesAllPages() {
        List<Bucket> buckets = AsyncBucketListingSupport.listAllBuckets(request -> {
            if (request.getContinuationToken() == null) {
                return CompletableFuture.completedFuture(
                        ListingPage.of(List.of(Bucket.builder().name("bucket-1").build()), "page-2")
                );
            }
            return CompletableFuture.completedFuture(
                    ListingPage.of(List.of(Bucket.builder().name("bucket-2").build()), null)
            );
        }).join();

        assertEquals(List.of("bucket-1", "bucket-2"), buckets.stream().map(Bucket::getName).toList());
    }
}
