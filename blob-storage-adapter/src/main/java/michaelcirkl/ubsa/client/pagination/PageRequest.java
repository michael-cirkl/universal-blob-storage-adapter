package michaelcirkl.ubsa.client.pagination;

/**
 * Parameters for requesting a single page from a paginated UBSA listing operation.
 *
 * <p>{@code pageSize} is optional and provider-dependent. {@code continuationToken} resumes listing from a
 * previously returned {@link ListingPage}.
 */
public final class PageRequest {
    private final Integer pageSize;
    private final String continuationToken;

    private PageRequest(Builder builder) {
        this.pageSize = validatePageSize(builder.pageSize);
        this.continuationToken = normalizeToken(builder.continuationToken);
    }

    /**
     * Creates a builder for a page request.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns a request for the first page using provider defaults.
     */
    public static PageRequest firstPage() {
        return builder().build();
    }

    /**
     * Returns the requested page size, or {@code null} to use the provider default.
     */
    public Integer getPageSize() {
        return pageSize;
    }

    /**
     * Returns the continuation token for the next page request, or {@code null} to start from the beginning.
     */
    public String getContinuationToken() {
        return continuationToken;
    }

    private static Integer validatePageSize(Integer pageSize) {
        if (pageSize != null && pageSize <= 0) {
            throw new IllegalArgumentException("Page size must be greater than 0.");
        }
        return pageSize;
    }

    private static String normalizeToken(String continuationToken) {
        if (continuationToken == null || continuationToken.isBlank()) {
            return null;
        }
        return continuationToken;
    }

    /**
     * Builder for {@link PageRequest}.
     */
    public static final class Builder {
        private Integer pageSize;
        private String continuationToken;

        /**
         * Sets the requested page size.
         */
        public Builder pageSize(Integer pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        /**
         * Sets the continuation token returned by a previous {@link ListingPage}.
         */
        public Builder continuationToken(String continuationToken) {
            this.continuationToken = continuationToken;
            return this;
        }

        /**
         * Builds the page request.
         */
        public PageRequest build() {
            return new PageRequest(this);
        }
    }
}
