package michaelcirkl.ubsa.client.pagination;

import java.util.List;
import java.util.Objects;
import java.util.Iterator;

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

    public List<T> getItems() {
        return items;
    }

    public String getNextContinuationToken() {
        return nextContinuationToken;
    }

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
