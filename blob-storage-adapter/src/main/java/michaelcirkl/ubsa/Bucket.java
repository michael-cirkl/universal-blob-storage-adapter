package michaelcirkl.ubsa;

import java.net.URI;
import java.time.LocalDateTime;

/**
 * Provider-neutral representation of bucket/container metadata.
 * Timestamp fields are normalized to UTC when the underlying provider exposes timezone-aware values.
 */
public class Bucket {
    private final String name;
    private final URI publicURI;
    private final LocalDateTime lastModified;
    private final LocalDateTime creationDate;

    private Bucket(Builder builder) {
        this.name = builder.name;
        this.publicURI = builder.publicURI;
        this.lastModified = builder.lastModified;
        this.creationDate = builder.creationDate;
    }

    public String getName() {
        return name;
    }

    /**
     * Returns a provider-specific URI for this bucket/container.
     *
     * <p>This value is intended for identification and linking. It does not guarantee anonymous/public access.
     */
    public URI getPublicURI() {
        return publicURI;
    }

    /**
     * Returns when the bucket metadata was last updated.
     */
    public LocalDateTime getLastModified() {
        return lastModified;
    }

    /**
     * Returns when the bucket/container was created.
     */
    public LocalDateTime getCreationDate() {
        return creationDate;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link Bucket}.
     */
    public static class Builder {
        private String name;
        private URI publicURI;
        private LocalDateTime lastModified;
        private LocalDateTime creationDate;

        /**
         * Sets the bucket/container name.
         *
         * @param name the bucket name
         */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * Sets the provider-specific bucket URI.
         *
         * @param publicURI the bucket URI
         */
        public Builder publicURI(URI publicURI) {
            this.publicURI = publicURI;
            return this;
        }

        /**
         * Sets the last-modified timestamp.
         *
         * @param lastModified the last modification time in UTC
         */
        public Builder lastModified(LocalDateTime lastModified) {
            this.lastModified = lastModified;
            return this;
        }

        /**
         * Sets the creation timestamp.
         *
         * @param creationDate the bucket creation time in UTC
         */
        public Builder creationDate(LocalDateTime creationDate) {
            this.creationDate = creationDate;
            return this;
        }

        public Bucket build() {
            return new Bucket(this);
        }
    }
}
