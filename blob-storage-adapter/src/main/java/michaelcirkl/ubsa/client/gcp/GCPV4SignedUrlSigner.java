package michaelcirkl.ubsa.client.gcp;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Storage;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.TreeMap;

class GCPV4SignedUrlSigner {
    private static final String HOST = "storage.googleapis.com";
    private static final String ALGORITHM = "GOOG4-RSA-SHA256";
    private static final long MAX_EXPIRY_SECONDS = 7L * 24L * 60L * 60L;
    private static final DateTimeFormatter DATE_STAMP = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter TIMESTAMP = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'");

    private GCPV4SignedUrlSigner() {
    }

    static URL generateGetUrl(Storage client, String bucket, String objectKey, Duration expiry) {
        return sign(client, "GET", bucket, objectKey, expiry, null);
    }

    static URL generatePutUrl(Storage client, String bucket, String objectKey, Duration expiry, String contentType) {
        String normalizedContentType = (contentType == null || contentType.isBlank()) ? null : contentType;
        return sign(client, "PUT", bucket, objectKey, expiry, normalizedContentType);
    }

    private static URL sign(Storage client, String method, String bucket, String objectKey, Duration expiry, String contentType) {
        ServiceAccountCredentials credentials = requireServiceAccountCredentials(client);
        long expirySeconds = GCPClientSupport.toPositiveSeconds(expiry);
        if (expirySeconds > MAX_EXPIRY_SECONDS) {
            throw new IllegalArgumentException("Expiry must not exceed 604800 seconds (7 days) for GCS V4 signed URLs.");
        }

        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        String datestamp = DATE_STAMP.format(now);
        String timestamp = TIMESTAMP.format(now);
        String credentialScope = datestamp + "/auto/storage/goog4_request";
        String canonicalUri = buildCanonicalUri(bucket, objectKey);
        String signedHeaders = "host";
        String canonicalHeaders = "host:" + HOST + "\n";

        Map<String, String> queryParameters = new TreeMap<>();
        queryParameters.put("X-Goog-Algorithm", ALGORITHM);
        queryParameters.put("X-Goog-Credential", credentials.getClientEmail() + "/" + credentialScope);
        queryParameters.put("X-Goog-Date", timestamp);
        queryParameters.put("X-Goog-Expires", Long.toString(expirySeconds));
        queryParameters.put("X-Goog-SignedHeaders", signedHeaders);

        String canonicalQuery = canonicalQuery(queryParameters);
        String canonicalRequest = method + "\n"
                + canonicalUri + "\n"
                + canonicalQuery + "\n"
                + canonicalHeaders + "\n"
                + signedHeaders + "\n"
                + "UNSIGNED-PAYLOAD";

        String stringToSign = ALGORITHM + "\n"
                + timestamp + "\n"
                + credentialScope + "\n"
                + sha256Hex(canonicalRequest);

        String signature = hex(credentials.sign(stringToSign.getBytes(StandardCharsets.UTF_8)));
        String signedUrl = "https://" + HOST + canonicalUri + "?" + canonicalQuery + "&X-Goog-Signature=" + signature;
        try {
            return URI.create(signedUrl).toURL();
        } catch (MalformedURLException error) {
            throw new IllegalArgumentException("Failed to build GCS signed URL.", error);
        }
    }

    private static ServiceAccountCredentials requireServiceAccountCredentials(Storage client) {
        if (client == null) {
            throw new IllegalArgumentException("Storage client must not be null.");
        }
        if (client.getOptions() == null || client.getOptions().getCredentials() == null) {
            throw new IllegalStateException("GCS signed URL generation requires credentials on the Storage client.");
        }
        if (client.getOptions().getCredentials() instanceof ServiceAccountCredentials serviceAccountCredentials) {
            return serviceAccountCredentials;
        }
        throw new IllegalStateException(
                "GCS signed URL generation requires service account credentials when using the gRPC transport. "
                        + "Set GOOGLE_APPLICATION_CREDENTIALS to a service account JSON key or provide ServiceAccountCredentials."
        );
    }

    private static String buildCanonicalUri(String bucket, String objectKey) {
        return "/" + encodePathSegment(bucket) + "/" + encodeObjectPath(objectKey);
    }

    private static String encodeObjectPath(String objectKey) {
        if (objectKey == null || objectKey.isEmpty()) {
            return "";
        }

        StringBuilder encoded = new StringBuilder();
        for (int index = 0; index < objectKey.length(); index++) {
            char current = objectKey.charAt(index);
            if (current == '/') {
                encoded.append('/');
            } else {
                appendEncoded(encoded, current);
            }
        }
        return encoded.toString();
    }

    private static String encodePathSegment(String value) {
        StringBuilder encoded = new StringBuilder();
        for (int index = 0; index < value.length(); index++) {
            appendEncoded(encoded, value.charAt(index));
        }
        return encoded.toString();
    }

    private static String canonicalQuery(Map<String, String> queryParameters) {
        StringBuilder canonical = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, String> entry : queryParameters.entrySet()) {
            if (!first) {
                canonical.append('&');
            }
            first = false;
            canonical.append(percentEncode(entry.getKey()));
            canonical.append('=');
            canonical.append(percentEncode(entry.getValue()));
        }
        return canonical.toString();
    }

    private static String percentEncode(String value) {
        StringBuilder encoded = new StringBuilder();
        for (byte current : value.getBytes(StandardCharsets.UTF_8)) {
            int unsigned = current & 0xff;
            if (isUnreserved(unsigned)) {
                encoded.append((char) unsigned);
            } else {
                encoded.append('%');
                encoded.append(Character.toUpperCase(Character.forDigit((unsigned >>> 4) & 0x0f, 16)));
                encoded.append(Character.toUpperCase(Character.forDigit(unsigned & 0x0f, 16)));
            }
        }
        return encoded.toString();
    }

    private static void appendEncoded(StringBuilder target, char value) {
        if (isUnreserved(value)) {
            target.append(value);
            return;
        }

        for (byte current : String.valueOf(value).getBytes(StandardCharsets.UTF_8)) {
            int unsigned = current & 0xff;
            target.append('%');
            target.append(Character.toUpperCase(Character.forDigit((unsigned >>> 4) & 0x0f, 16)));
            target.append(Character.toUpperCase(Character.forDigit(unsigned & 0x0f, 16)));
        }
    }

    private static boolean isUnreserved(int value) {
        return (value >= 'A' && value <= 'Z')
                || (value >= 'a' && value <= 'z')
                || (value >= '0' && value <= '9')
                || value == '-'
                || value == '_'
                || value == '.'
                || value == '~';
    }

    private static String sha256Hex(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return hex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is not available.", error);
        }
    }

    private static String hex(byte[] value) {
        StringBuilder output = new StringBuilder(value.length * 2);
        for (byte current : value) {
            int unsigned = current & 0xff;
            output.append(Character.forDigit((unsigned >>> 4) & 0x0f, 16));
            output.append(Character.forDigit(unsigned & 0x0f, 16));
        }
        return output.toString();
    }
}
