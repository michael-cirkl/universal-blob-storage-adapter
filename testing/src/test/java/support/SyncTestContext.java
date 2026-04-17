package support;

import io.github.michaelcirkl.ubsa.BlobStorageSyncClient;
import io.github.michaelcirkl.ubsa.Bucket;
import io.github.michaelcirkl.ubsa.Provider;

import java.io.IOException;
import java.net.URL;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public final class SyncTestContext implements AutoCloseable {
    private final SyncProviderFixture fixture;
    private final BlobStorageSyncClient client;
    private final List<String> trackedBuckets = new ArrayList<>();

    SyncTestContext(SyncProviderFixture fixture) {
        this.fixture = fixture;
        this.client = fixture.client();
    }

    public Provider provider() {
        return fixture.provider();
    }

    public BlobStorageSyncClient client() {
        return client;
    }

    public String createBucket(String prefix) {
        String bucketName = newBucketName(prefix);
        client.createBucket(Bucket.builder().name(bucketName).build());
        trackedBuckets.add(bucketName);
        return bucketName;
    }

    public String newBucketName(String prefix) {
        String normalized = prefix == null ? "bucket" : prefix.toLowerCase().replaceAll("[^a-z0-9]", "");
        if (normalized.isBlank()) {
            normalized = "bucket";
        }
        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 18);
        String combined = normalized + suffix;
        return combined.substring(0, Math.min(combined.length(), 63));
    }

    public byte[] readRequired(URL url) throws IOException, InterruptedException {
        return fixture.readRequired(url);
    }

    public void writeRequired(URL url, String contentType, byte[] payload) throws IOException, InterruptedException {
        fixture.writeRequired(url, contentType, payload);
    }

    public HttpResponse<byte[]> getSignedUrlResponse(URL url) throws IOException, InterruptedException {
        return fixture.getSignedUrlResponse(url);
    }

    public HttpResponse<byte[]> putSignedUrlResponse(URL url, String contentType, byte[] payload) throws IOException, InterruptedException {
        return fixture.putSignedUrlResponse(url, contentType, payload);
    }

    @Override
    public void close() {
        RuntimeException cleanupError = null;
        for (int i = trackedBuckets.size() - 1; i >= 0; i--) {
            String bucketName = trackedBuckets.get(i);
            try {
                fixture.cleanupBucket(bucketName);
            } catch (RuntimeException error) {
                if (cleanupError == null) {
                    cleanupError = error;
                } else {
                    cleanupError.addSuppressed(error);
                }
            }
        }

        try {
            fixture.close();
        } catch (RuntimeException error) {
            if (cleanupError == null) {
                cleanupError = error;
            } else {
                cleanupError.addSuppressed(error);
            }
        }

        if (cleanupError != null) {
            throw cleanupError;
        }
    }
}
