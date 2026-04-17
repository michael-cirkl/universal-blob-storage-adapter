package io.github.michaelcirkl.ubsa.client.streaming;

import java.util.Map;

/**
 * Optional metadata and content settings applied when creating or uploading a blob using streaming.
 */
public class BlobWriteOptions {
    private final String encoding;
    private final Map<String, String> userMetadata;

    private BlobWriteOptions(Builder builder) {
        this.encoding = builder.encoding;
        this.userMetadata = builder.userMetadata;
    }

    /**
     * Returns the content encoding to store with the blob.
     */
    public String encoding() {
        return encoding;
    }

    /**
     * Returns user-defined metadata to store with the blob.
     */
    public Map<String, String> userMetadata() {
        return userMetadata;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String encoding;
        private Map<String, String> userMetadata;

        /**
         * Sets the content encoding to store with the blob.
         */
        public Builder encoding(String encoding) {
            this.encoding = encoding;
            return this;
        }

        /**
         * Sets user-defined metadata to store with the blob.
         */
        public Builder userMetadata(Map<String, String> userMetadata) {
            this.userMetadata = userMetadata;
            return this;
        }

        public BlobWriteOptions build() {
            return new BlobWriteOptions(this);
        }
    }
}
