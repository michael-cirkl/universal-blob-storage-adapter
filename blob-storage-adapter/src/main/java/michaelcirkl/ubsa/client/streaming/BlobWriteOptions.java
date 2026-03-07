package michaelcirkl.ubsa.client.streaming;

import java.util.Map;

public class BlobWriteOptions {
    private final String encoding;
    private final Map<String, String> userMetadata;

    private BlobWriteOptions(Builder builder) {
        this.encoding = builder.encoding;
        this.userMetadata = builder.userMetadata;
    }

    public String encoding() {
        return encoding;
    }

    public Map<String, String> userMetadata() {
        return userMetadata;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String encoding;
        private Map<String, String> userMetadata;

        public Builder encoding(String encoding) {
            this.encoding = encoding;
            return this;
        }

        public Builder userMetadata(Map<String, String> userMetadata) {
            this.userMetadata = userMetadata;
            return this;
        }

        public BlobWriteOptions build() {
            return new BlobWriteOptions(this);
        }
    }
}
