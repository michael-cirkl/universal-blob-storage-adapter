package michaelcirkl.ubsa;

import java.net.URI;
import java.time.LocalDateTime;
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

    public URI getPublicURI() {
        return publicURI;
    }

    public LocalDateTime getLastModified() {
        return lastModified;
    }

    public LocalDateTime getCreationDate() {
        return creationDate;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String name;
        private URI publicURI;
        private LocalDateTime lastModified;
        private LocalDateTime creationDate;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder publicURI(URI publicURI) {
            this.publicURI = publicURI;
            return this;
        }

        public Builder lastModified(LocalDateTime lastModified) {
            this.lastModified = lastModified;
            return this;
        }

        public Builder creationDate(LocalDateTime creationDate) {
            this.creationDate = creationDate;
            return this;
        }

        public Bucket build() {
            return new Bucket(this);
        }
    }
}
