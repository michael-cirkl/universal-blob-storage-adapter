package michaelcirkl.ubsa.client.util;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.HttpMethod;
import com.google.cloud.storage.Storage;
import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.client.gcp.GCPClientSupport;
import michaelcirkl.ubsa.client.pagination.PageRequest;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GCPClientSupportTest {
    @Test
    void mapsBucketsBlobsAndFetchedBlobValues() throws Exception {
        OffsetDateTime bucketCreated = OffsetDateTime.of(2025, 3, 1, 10, 15, 0, 0, ZoneOffset.ofHours(2));
        OffsetDateTime bucketUpdated = OffsetDateTime.of(2025, 3, 2, 11, 30, 0, 0, ZoneOffset.ofHours(2));
        com.google.cloud.storage.Bucket gcsBucket = toSdkBucket(
                buildBucketInfo("bucket-a", bucketCreated, bucketUpdated)
        );

        List<Bucket> buckets = GCPClientSupport.mapBuckets(List.of(gcsBucket));
        assertEquals(1, buckets.size());
        assertEquals("bucket-a", buckets.getFirst().getName());
        assertEquals(URI.create("gs://bucket-a"), buckets.getFirst().getPublicURI());
        assertEquals(LocalDateTime.of(2025, 3, 1, 8, 15), buckets.getFirst().getCreationDate());
        assertEquals(LocalDateTime.of(2025, 3, 2, 9, 30), buckets.getFirst().getLastModified());

        OffsetDateTime blobUpdated = OffsetDateTime.of(2025, 3, 3, 9, 45, 0, 0, ZoneOffset.ofHours(-4));
        BlobInfo blobInfo = buildBlobInfo("bucket-a", "path/blob.txt", 5L, "etag-123", "gzip", Map.of("k", "v"), blobUpdated);
        com.google.cloud.storage.Blob gcsBlob = toSdkBlob(blobInfo);

        List<Blob> blobs = GCPClientSupport.mapBlobsFromPage("bucket-a", List.of(gcsBlob));
        assertEquals(1, blobs.size());
        Blob listed = blobs.getFirst();
        assertEquals("bucket-a", listed.getBucket());
        assertEquals("path/blob.txt", listed.getKey());
        assertEquals(5L, listed.getSize());
        assertEquals("etag-123", listed.getEtag());
        assertEquals("gzip", listed.encoding());
        assertEquals(Map.of("k", "v"), listed.getUserMetadata());
        assertEquals(URI.create("gs://bucket-a/path/blob.txt"), listed.getPublicURI());
        assertEquals(LocalDateTime.of(2025, 3, 3, 13, 45), listed.lastModified());

        Blob fetched = GCPClientSupport.mapFetchedBlob("bucket-a", "path/blob.txt", blobInfo, "hello".getBytes());
        assertArrayEquals("hello".getBytes(), fetched.getContent());
        assertEquals(5L, fetched.getSize());
        assertEquals("etag-123", fetched.getEtag());
        assertEquals(Map.of("k", "v"), fetched.getUserMetadata());
        assertEquals(URI.create("gs://bucket-a/path/blob.txt"), fetched.getPublicURI());
    }

    @Test
    void buildsBucketAndBlobListOptionsFromPageRequest() {
        PageRequest request = PageRequest.builder()
                .pageSize(25)
                .continuationToken("next-page")
                .build();

        assertArrayEquals(
                new Storage.BucketListOption[]{
                        Storage.BucketListOption.pageSize(25),
                        Storage.BucketListOption.pageToken("next-page")
                },
                GCPClientSupport.buildBucketListOptions(request)
        );
        assertArrayEquals(
                new Storage.BlobListOption[]{
                        Storage.BlobListOption.prefix("logs/"),
                        Storage.BlobListOption.pageSize(25),
                        Storage.BlobListOption.pageToken("next-page")
                },
                GCPClientSupport.buildBlobListOptions("logs/", request)
        );
        assertArrayEquals(
                new Storage.BlobListOption[]{
                        Storage.BlobListOption.pageSize(25),
                        Storage.BlobListOption.pageToken("next-page")
                },
                GCPClientSupport.buildBlobListOptions("  ", request)
        );
    }

    @Test
    void normalizesPageRequestAndGsUris() {
        PageRequest normalized = GCPClientSupport.normalizePageRequest(null);
        assertNull(normalized.getPageSize());
        assertNull(normalized.getContinuationToken());
        assertEquals(URI.create("gs://bucket"), GCPClientSupport.toGsUri("bucket", null));
        assertEquals(URI.create("gs://bucket/blob"), GCPClientSupport.toGsUri("bucket", "blob"));
    }

    @Test
    void validatesExpiry() {
        assertThrows(IllegalArgumentException.class, () -> GCPClientSupport.toPositiveSeconds(null));
        assertThrows(IllegalArgumentException.class, () -> GCPClientSupport.toPositiveSeconds(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> GCPClientSupport.toPositiveSeconds(Duration.ofSeconds(-1)));
        assertEquals(90L, GCPClientSupport.toPositiveSeconds(Duration.ofSeconds(90)));
    }

    @Test
    void generatesSignedGetUrl() throws Exception {
        URL expected = new URL("https://example.test/get");
        SignUrlCapture capture = new SignUrlCapture(expected);

        URL actual = GCPClientSupport.generateGetUrl(capture.storage(), "bucket-a", "blob-a", Duration.ofMinutes(2));

        assertEquals(expected, actual);
        assertEquals("bucket-a", capture.blobInfo.getBucket());
        assertEquals("blob-a", capture.blobInfo.getName());
        assertEquals(120L, capture.duration);
        assertEquals(TimeUnit.SECONDS, capture.unit);
        assertEquals(0, capture.options.length);
    }

    @Test
    void generatesSignedPutUrlWithAndWithoutContentType() throws Exception {
        SignUrlCapture withContentTypeCapture = new SignUrlCapture(new URL("https://example.test/put-with-type"));
        URL withContentType = GCPClientSupport.generatePutUrl(
                withContentTypeCapture.storage(),
                "bucket-a",
                "blob-a",
                Duration.ofMinutes(5),
                "text/plain"
        );

        assertEquals(new URL("https://example.test/put-with-type"), withContentType);
        assertEquals("text/plain", withContentTypeCapture.blobInfo.getContentType());
        assertEquals(300L, withContentTypeCapture.duration);
        assertEquals(TimeUnit.SECONDS, withContentTypeCapture.unit);
        assertSignUrlOptions(
                new Storage.SignUrlOption[]{
                        Storage.SignUrlOption.httpMethod(HttpMethod.PUT),
                        Storage.SignUrlOption.withContentType()
                },
                withContentTypeCapture.options
        );

        SignUrlCapture withoutContentTypeCapture = new SignUrlCapture(new URL("https://example.test/put-without-type"));
        URL withoutContentType = GCPClientSupport.generatePutUrl(
                withoutContentTypeCapture.storage(),
                "bucket-a",
                "blob-a",
                Duration.ofMinutes(5),
                " "
        );

        assertEquals(new URL("https://example.test/put-without-type"), withoutContentType);
        assertNull(withoutContentTypeCapture.blobInfo.getContentType());
        assertSignUrlOptions(
                new Storage.SignUrlOption[]{
                        Storage.SignUrlOption.httpMethod(HttpMethod.PUT)
                },
                withoutContentTypeCapture.options
        );
    }

    private static BucketInfo buildBucketInfo(String name, OffsetDateTime created, OffsetDateTime updated) throws Exception {
        BucketInfo.Builder builder = BucketInfo.newBuilder(name);
        invokeMethod(builder, "setCreateTimeOffsetDateTime", new Class[]{OffsetDateTime.class}, created);
        invokeMethod(builder, "setUpdateTimeOffsetDateTime", new Class[]{OffsetDateTime.class}, updated);
        return builder.build();
    }

    private static BlobInfo buildBlobInfo(
            String bucket,
            String name,
            long size,
            String etag,
            String encoding,
            Map<String, String> metadata,
            OffsetDateTime updated
    ) throws Exception {
        BlobInfo.Builder builder = BlobInfo.newBuilder(bucket, name)
                .setContentEncoding(encoding)
                .setMetadata(metadata);
        invokeMethod(builder, "setSize", new Class[]{Long.class}, size);
        invokeMethod(builder, "setEtag", new Class[]{String.class}, etag);
        invokeMethod(builder, "setUpdateTimeOffsetDateTime", new Class[]{OffsetDateTime.class}, updated);
        return builder.build();
    }

    private static com.google.cloud.storage.Bucket toSdkBucket(BucketInfo bucketInfo) throws Exception {
        return (com.google.cloud.storage.Bucket) invokeMethod(bucketInfo, "asBucket", new Class[]{Storage.class}, noopStorage());
    }

    private static com.google.cloud.storage.Blob toSdkBlob(BlobInfo blobInfo) throws Exception {
        return (com.google.cloud.storage.Blob) invokeMethod(blobInfo, "asBlob", new Class[]{Storage.class}, noopStorage());
    }

    private static void assertSignUrlOptions(Storage.SignUrlOption[] expected, Storage.SignUrlOption[] actual) throws Exception {
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(invokeMethod(expected[i], "getOption", new Class[0]), invokeMethod(actual[i], "getOption", new Class[0]));
            assertEquals(invokeMethod(expected[i], "getValue", new Class[0]), invokeMethod(actual[i], "getValue", new Class[0]));
        }
    }

    private static Object invokeMethod(Object target, String name, Class<?>[] parameterTypes, Object... args) throws Exception {
        Method method = findMethod(target.getClass(), name, parameterTypes);
        method.setAccessible(true);
        return method.invoke(target, args);
    }

    private static Method findMethod(Class<?> type, String name, Class<?>[] parameterTypes) throws NoSuchMethodException {
        Class<?> current = type;
        while (current != null) {
            try {
                return current.getDeclaredMethod(name, parameterTypes);
            } catch (NoSuchMethodException ignored) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchMethodException(name);
    }

    private static Storage noopStorage() {
        InvocationHandler handler = (proxy, method, args) -> defaultValue(method.getReturnType());
        return (Storage) Proxy.newProxyInstance(Storage.class.getClassLoader(), new Class[]{Storage.class}, handler);
    }

    private static Object defaultValue(Class<?> returnType) {
        if (!returnType.isPrimitive()) {
            return null;
        }
        if (returnType == boolean.class) {
            return false;
        }
        if (returnType == char.class) {
            return '\0';
        }
        if (returnType == byte.class) {
            return (byte) 0;
        }
        if (returnType == short.class) {
            return (short) 0;
        }
        if (returnType == int.class) {
            return 0;
        }
        if (returnType == long.class) {
            return 0L;
        }
        if (returnType == float.class) {
            return 0f;
        }
        if (returnType == double.class) {
            return 0d;
        }
        return null;
    }

    private static final class SignUrlCapture implements InvocationHandler {
        private final URL returnedUrl;
        private BlobInfo blobInfo;
        private long duration;
        private TimeUnit unit;
        private Storage.SignUrlOption[] options = new Storage.SignUrlOption[0];

        private SignUrlCapture(URL returnedUrl) {
            this.returnedUrl = returnedUrl;
        }

        private Storage storage() {
            return (Storage) Proxy.newProxyInstance(
                    Storage.class.getClassLoader(),
                    new Class[]{Storage.class},
                    this
            );
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            if (method.getName().equals("signUrl")) {
                blobInfo = (BlobInfo) args[0];
                duration = ((Number) args[1]).longValue();
                unit = (TimeUnit) args[2];
                options = args.length > 3
                        ? (Storage.SignUrlOption[]) Objects.requireNonNullElse(args[3], new Storage.SignUrlOption[0])
                        : new Storage.SignUrlOption[0];
                return returnedUrl;
            }
            if (method.getName().equals("hashCode")) {
                return System.identityHashCode(proxy);
            }
            if (method.getName().equals("equals")) {
                return proxy == args[0];
            }
            if (method.getName().equals("toString")) {
                return "SignUrlCaptureStorage";
            }
            return defaultValue(method.getReturnType());
        }
    }
}
