package michaelcirkl.ubsa.client.pagination;

import michaelcirkl.ubsa.Bucket;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public final class BucketListingSupport {
    private BucketListingSupport() {
    }

    public static List<Bucket> listAllBuckets(Function<PageRequest, ListingPage<Bucket>> pageLoader) {
        List<Bucket> buckets = new ArrayList<>();
        PageRequest nextRequest = PageRequest.firstPage();
        while (true) {
            ListingPage<Bucket> page = pageLoader.apply(nextRequest);
            buckets.addAll(page.getItems());
            if (!page.hasNextPage()) {
                return List.copyOf(buckets);
            }
            nextRequest = PageRequest.builder()
                    .pageSize(nextRequest.getPageSize())
                    .continuationToken(page.getNextContinuationToken())
                    .build();
        }
    }
}
