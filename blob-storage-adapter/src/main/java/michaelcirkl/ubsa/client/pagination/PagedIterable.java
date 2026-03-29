package michaelcirkl.ubsa.client.pagination;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * Lazily iterates across all items produced by a paginated listing function.
 *
 * <p>Pages are loaded on demand as iteration advances, reusing the previous page size and the continuation
 * token returned by each {@link ListingPage}.
 */
public final class PagedIterable<T> implements Iterable<T> {
    private final PageRequest initialRequest;
    private final Function<PageRequest, ListingPage<T>> pageLoader;

    /**
     * Creates an iterable backed by the given page loader.
     *
     * <p>If {@code initialRequest} is {@code null}, iteration starts from {@link PageRequest#firstPage()}.
     */
    public PagedIterable(
            PageRequest initialRequest,
            Function<PageRequest, ListingPage<T>> pageLoader
    ) {
        this.initialRequest = initialRequest == null ? PageRequest.firstPage() : initialRequest;
        this.pageLoader = pageLoader;
    }

    /**
     * Returns an iterator that loads additional pages only when needed.
     */
    @Override
    public Iterator<T> iterator() {
        return new Iterator<>() {
            private PageRequest nextRequest = initialRequest;
            private Iterator<T> currentItems = Collections.emptyIterator();
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
