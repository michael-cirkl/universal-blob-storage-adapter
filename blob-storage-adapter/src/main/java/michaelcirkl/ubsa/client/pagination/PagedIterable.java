package michaelcirkl.ubsa.client.pagination;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

public final class PagedIterable<T> implements Iterable<T> {
    private final PageRequest initialRequest;
    private final Function<PageRequest, ListingPage<T>> pageLoader;

    public PagedIterable(
            PageRequest initialRequest,
            Function<PageRequest, ListingPage<T>> pageLoader
    ) {
        this.initialRequest = initialRequest == null ? PageRequest.firstPage() : initialRequest;
        this.pageLoader = pageLoader;
    }

    @Override
    public Iterator<T> iterator() {
        return new Iterator<>() {
            private PageRequest nextRequest = initialRequest;
            private Iterator<T> currentItems = List.<T>of().iterator();
            private boolean exhausted;

            @Override
            public boolean hasNext() {
                loadUntilItemAvailable();
                return currentItems.hasNext();
            }

            @Override
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more items available.");
                }
                return currentItems.next();
            }

            private void loadUntilItemAvailable() {
                while (!currentItems.hasNext() && !exhausted) {
                    ListingPage<T> page = pageLoader.apply(nextRequest);
                    currentItems = page.iterator();
                    if (page.hasNextPage()) {
                        nextRequest = PageRequest.builder()
                                .pageSize(nextRequest.getPageSize())
                                .continuationToken(page.getNextContinuationToken())
                                .build();
                    } else {
                        exhausted = true;
                    }
                }
            }
        };
    }
}
