package michaelcirkl.ubsa;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.Map;

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

    public byte[] getContent() {
        return content;
    }

    public long getSize() {
        return size;
    }

    public String getKey() {
        return key;
    }

    public LocalDateTime lastModified() {
        return lastModified;
    }

    public String encoding() {
        return encoding;
    }

    public String getEtag() {
        return etag;
    }

    public Map<String, String> getUserMetadata() {
        return userMetadata;
    }

    public URI getPublicURI() {
        return publicURI;
    }

    public String getBucket() {
        return bucket;
    }

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

        public Builder content(byte[] content) {
            this.content = content;
            return this;
        }

        public Builder size(long size) {
            this.size = size;
            return this;
        }

        public Builder key(String key) {
            this.key = key;
            return this;
        }

        public Builder lastModified(LocalDateTime lastModified) {
            this.lastModified = lastModified;
            return this;
        }

        public Builder encoding(String encoding) {
            this.encoding = encoding;
            return this;
        }

        public Builder etag(String etag) {
            this.etag = etag;
            return this;
        }

        public Builder userMetadata(Map<String, String> userMetadata) {
            this.userMetadata = userMetadata;
            return this;
        }

        public Builder publicURI(URI publicURI) {
            this.publicURI = publicURI;
            return this;
        }

        public Builder bucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public Builder expires(LocalDateTime expires) {
            this.expires = expires;
            return this;
        }

        public Blob build() {
            return new Blob(this);
        }
    }
}
