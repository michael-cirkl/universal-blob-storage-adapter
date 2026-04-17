package io.github.michaelcirkl.ubsa.client.pagination;

import java.util.List;
import java.util.Objects;
import java.util.Iterator;

/**
 * A single page of listing results returned by a paginated UBSA operation.
 *
 * <p>The page contains the current items and, when available, the continuation token needed to request
 * the next page.
 */
public final class ListingPage<T> implements Iterable<T> {
    private final List<T> items;
    private final String nextContinuationToken;
    private final boolean hasNextPage;

    private ListingPage(List<T> items, String nextContinuationToken) {
        this.items = List.copyOf(Objects.requireNonNull(items, "items must not be null"));
        this.nextContinuationToken = normalizeToken(nextContinuationToken);
        this.hasNextPage = this.nextContinuationToken != null;
    }

    public static <T> ListingPage<T> of(List<T> items, String nextContinuationToken) {
        return new ListingPage<>(items, nextContinuationToken);
    }

    /**
     * Returns the items in the current page.
     */
    public List<T> getItems() {
        return items;
    }

    /**
     * Returns the token to pass into the next {@link PageRequest}, or {@code null} when there is no next page.
     */
    public String getNextContinuationToken() {
        return nextContinuationToken;
    }

    /**
     * Returns whether another page is available.
     */
    public boolean hasNextPage() {
        return hasNextPage;
    }

    @Override
    public Iterator<T> iterator() {
        return items.iterator();
    }

    private static String normalizeToken(String continuationToken) {
        if (continuationToken == null || continuationToken.isBlank()) {
            return null;
        }
        return continuationToken;
    }
}
