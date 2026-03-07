package michaelcirkl.ubsa.client.async;

import java.time.LocalDateTime;
import java.util.Map;

public class BlobWriteOptions {
    private final String encoding;
    private final Map<String, String> userMetadata;
    private final LocalDateTime expires;

    private BlobWriteOptions(Builder builder) {
        this.encoding = builder.encoding;
        this.userMetadata = builder.userMetadata;
        this.expires = builder.expires;
    }

    public String encoding() {
        return encoding;
    }

    public Map<String, String> userMetadata() {
        return userMetadata;
    }

    public LocalDateTime expires() {
        return expires;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String encoding;
        private Map<String, String> userMetadata;
        private LocalDateTime expires;

        public Builder encoding(String encoding) {
            this.encoding = encoding;
            return this;
        }

        public Builder userMetadata(Map<String, String> userMetadata) {
            this.userMetadata = userMetadata;
            return this;
        }

        public Builder expires(LocalDateTime expires) {
            this.expires = expires;
            return this;
        }

        public BlobWriteOptions build() {
            return new BlobWriteOptions(this);
        }
    }
}
