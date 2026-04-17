package io.github.michaelcirkl.ubsa;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * Provider-neutral representation of a blob/object and its associated metadata.
 *
 * <p>Instances returned by listing or metadata-only operations may omit {@link #getContent()}.
 * Timestamp fields are stored in UTC.
 */
public class Blob {
    private final byte[] content;
    private final long size;
    private final String key;
    private final LocalDateTime lastModified;
    private final String encoding;
    private final String etag;
    private final Map<String, String> userMetadata;
    private final URI publicURI;
    private final String bucket;
    private final LocalDateTime expires;

    private Blob(Builder builder) {
        this.content = builder.content;
        this.size = builder.size;
        this.key = builder.key;
        this.lastModified = builder.lastModified;
        this.encoding = builder.encoding;
        this.etag = builder.etag;
        this.userMetadata = builder.userMetadata;
        this.publicURI = builder.publicURI;
        this.bucket = builder.bucket;
        this.expires = builder.expires;
    }

    /**
     * Returns the blob content loaded into memory.
     *
     * <p>{@link BlobStorageSyncClient#getBlobMetadata(String, String)},
     * {@link BlobStorageAsyncClient#getBlobMetadata(String, String)}, listing operations,
     * and other metadata-only reads return {@code null}.
     */
    public byte[] getContent() {
        return content;
    }

    /**
     * Returns the blob size in bytes.
     */
    public long getSize() {
        return size;
    }

    public String getKey() {
        return key;
    }
    
    /**
     * Returns the provider-reported last-modified timestamp.
     *
     * <p>UBSA converts timezone-aware provider timestamps to UTC.
     */
    public LocalDateTime lastModified() {
        return lastModified;
    }

    /**
     * Returns the content encoding reported by the provider.
     */
    public String encoding() {
        return encoding;
    }

    /**
     * Returns the provider ETag, typically an identifier for a specific version of the blob content.
     */
    public String getEtag() {
        return etag;
    }

    /**
     * Returns user-defined metadata attached to the blob.
     */
    public Map<String, String> getUserMetadata() {
        return userMetadata;
    }

    /**
     * Returns a provider-specific URI for this blob.
     *
     * <p>This value is intended for identification and linking. It does not guarantee anonymous/public access.
     */
    public URI getPublicURI() {
        return publicURI;
    }

    public String getBucket() {
        return bucket;
    }

    /**
     * Returns the blob expiry timestamp when the provider exposes it.
     *
     * <p>UBSA converts timezone-aware provider timestamps to UTC.
     */
    public LocalDateTime expires() {
        return expires;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private byte[] content;
        private long size;
        private String key;
        private LocalDateTime lastModified;
        private String encoding;
        private String etag;
        private Map<String, String> userMetadata;
        private URI publicURI;
        private String bucket;
        private LocalDateTime expires;

        /**
         * Sets the in-memory blob payload.
         *
         * @param content the blob bytes
         */
        public Builder content(byte[] content) {
            this.content = content;
            return this;
        }

        /**
         * Sets the blob size in bytes.
         *
         * @param size the content length in bytes
         */
        public Builder size(long size) {
            this.size = size;
            return this;
        }

        /**
         * Sets the provider object key.
         *
         * @param key the blob key within the bucket
         */
        public Builder key(String key) {
            this.key = key;
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
         * Sets the provider-reported content encoding.
         *
         * @param encoding the content encoding
         */
        public Builder encoding(String encoding) {
            this.encoding = encoding;
            return this;
        }

        /**
         * Sets the provider ETag, typically an identifier for a specific version of the blob content.
         *
         * @param etag the ETag value
         */
        public Builder etag(String etag) {
            this.etag = etag;
            return this;
        }

        /**
         * Sets user-defined metadata entries.
         *
         * @param userMetadata the metadata map
         */
        public Builder userMetadata(Map<String, String> userMetadata) {
            this.userMetadata = userMetadata;
            return this;
        }

        /**
         * Sets the provider-specific blob URI.
         *
         * @param publicURI the blob URI
         */
        public Builder publicURI(URI publicURI) {
            this.publicURI = publicURI;
            return this;
        }

        /**
         * Sets the owning bucket or container name.
         *
         * @param bucket the bucket name
         */
        public Builder bucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        /**
         * Sets the blob expiry timestamp.
         *
         * @param expires the expiry timestamp in UTC
         */
        public Builder expires(LocalDateTime expires) {
            this.expires = expires;
            return this;
        }

        public Blob build() {
            return new Blob(this);
        }
    }
}
