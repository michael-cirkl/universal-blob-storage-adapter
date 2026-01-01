package michaelcirkl.ubsa;

import com.google.common.hash.HashCode;

import java.net.URI;
import java.time.LocalDateTime;

public interface Blob {
   String getName();
   byte[] getContent();
   long getSize();
   String getKey();
   LocalDateTime lastModified();
   String encoding();
   HashCode contentMD5();
   URI getPublicURI();
   String getBucket();
   void getTier();
   LocalDateTime expires();
}
