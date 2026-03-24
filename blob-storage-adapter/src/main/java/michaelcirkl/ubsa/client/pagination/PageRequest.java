package michaelcirkl.ubsa.client.pagination;

public final class PageRequest {
    private final Integer pageSize;
    private final String continuationToken;

    private PageRequest(Builder builder) {
        this.pageSize = validatePageSize(builder.pageSize);
        this.continuationToken = normalizeToken(builder.continuationToken);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static PageRequest firstPage() {
        return builder().build();
    }

    public Integer getPageSize() {
        return pageSize;
    }

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

    public static final class Builder {
        private Integer pageSize;
        private String continuationToken;

        public Builder pageSize(Integer pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public Builder continuationToken(String continuationToken) {
            this.continuationToken = continuationToken;
            return this;
        }

        public PageRequest build() {
            return new PageRequest(this);
        }
    }
}
