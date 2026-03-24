package michaelcirkl.ubsa.client.pagination;

import michaelcirkl.ubsa.Bucket;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BucketListingSupportTest {
    @Test
    void listAllBucketsTraversesAllPages() {
        List<Bucket> buckets = BucketListingSupport.listAllBuckets(request -> {
            if (request.getContinuationToken() == null) {
                return ListingPage.of(List.of(Bucket.builder().name("bucket-1").build()), "page-2");
            }
            return ListingPage.of(List.of(Bucket.builder().name("bucket-2").build()), null);
        });

        assertEquals(List.of("bucket-1", "bucket-2"), buckets.stream().map(Bucket::getName).toList());
    }
}
