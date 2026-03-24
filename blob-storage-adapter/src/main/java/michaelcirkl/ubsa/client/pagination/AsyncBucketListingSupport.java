package michaelcirkl.ubsa.client.pagination;

import michaelcirkl.ubsa.Bucket;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public final class AsyncBucketListingSupport {
    private AsyncBucketListingSupport() {
    }

    public static CompletableFuture<List<Bucket>> listAllBuckets(
            Function<PageRequest, CompletableFuture<ListingPage<Bucket>>> pageLoader
    ) {
        CompletableFuture<List<Bucket>> result = new CompletableFuture<>();
        collectAllBuckets(pageLoader, PageRequest.firstPage(), new ArrayList<>(), result);
        return result;
    }

    private static void collectAllBuckets(
            Function<PageRequest, CompletableFuture<ListingPage<Bucket>>> pageLoader,
            PageRequest request,
            List<Bucket> accumulatedBuckets,
            CompletableFuture<List<Bucket>> result
    ) {
        if (result.isDone()) {
            return;
        }
        CompletableFuture<ListingPage<Bucket>> pageFuture;
        try {
            pageFuture = pageLoader.apply(request);
        } catch (Throwable error) {
            result.completeExceptionally(error);
            return;
        }
        if (pageFuture == null) {
            result.completeExceptionally(new NullPointerException("pageLoader must not return null"));
            return;
        }
        pageFuture.whenComplete((page, error) -> {
            if (error != null) {
                result.completeExceptionally(error);
                return;
            }
            ListingPage<Bucket> safePage = page == null ? ListingPage.of(List.of(), null) : page;
            accumulatedBuckets.addAll(safePage.getItems());
            if (!safePage.hasNextPage()) {
                result.complete(List.copyOf(accumulatedBuckets));
                return;
            }
            PageRequest nextRequest = PageRequest.builder()
                    .pageSize(request.getPageSize())
                    .continuationToken(safePage.getNextContinuationToken())
                    .build();
            collectAllBuckets(pageLoader, nextRequest, accumulatedBuckets, result);
        });
    }
}
