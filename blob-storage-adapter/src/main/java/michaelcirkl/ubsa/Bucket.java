package michaelcirkl.ubsa;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.Map;

public interface Bucket {
    String getName();
    URI getPublicURI();
    Map<String, String> getUserMetadata();
    long getSize();
    String getEtag();
    void getTier();
    LocalDateTime getLastModified();
    LocalDateTime getCreationDate();
}
