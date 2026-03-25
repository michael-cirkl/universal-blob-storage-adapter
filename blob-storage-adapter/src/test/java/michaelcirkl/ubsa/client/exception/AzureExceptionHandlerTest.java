package michaelcirkl.ubsa.client.exception;

import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.storage.blob.models.BlobStorageException;
import michaelcirkl.ubsa.client.exception.types.AccessDeniedException;
import michaelcirkl.ubsa.client.exception.types.AuthenticationFailedException;
import michaelcirkl.ubsa.client.exception.types.BlobNotFoundException;
import michaelcirkl.ubsa.client.exception.types.BucketAlreadyExistsException;
import michaelcirkl.ubsa.client.exception.types.BucketNotFoundException;
import michaelcirkl.ubsa.client.exception.types.ConditionFailedException;
import michaelcirkl.ubsa.client.exception.types.InvalidRequestException;
import michaelcirkl.ubsa.client.exception.types.RequestTimeoutException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class AzureExceptionHandlerTest {
    private final AzureExceptionHandler handler = new AzureExceptionHandler();

    @Test
    void mapsKnownAzureCodesToTypedUbsaExceptions() {
        assertMapped("ContainerNotFound", 404, BucketNotFoundException.class);
        assertMapped("BlobNotFound", 404, BlobNotFoundException.class);
        assertMapped("ContainerAlreadyExists", 409, BucketAlreadyExistsException.class);
        assertMapped("ConditionNotMet", 412, ConditionFailedException.class);
        assertMapped("AuthenticationFailed", 401, AuthenticationFailedException.class);
        assertMapped("AuthorizationPermissionMismatch", 403, AccessDeniedException.class);
        assertMapped("OperationTimedOut", 408, RequestTimeoutException.class);
        assertMapped("InvalidQueryParameterValue", 400, InvalidRequestException.class);
    }

    @Test
    void wrapsUnknownAzureCodesAsGenericUbsaException() {
        RuntimeException actual = handler.propagate(blobStorageException("ServerBusy", 503));

        assertEquals(UbsaException.class, actual.getClass());
        assertEquals(503, ((UbsaException) actual).getStatusCode());
    }

    private void assertMapped(String code, int statusCode, Class<? extends UbsaException> expectedType) {
        RuntimeException actual = handler.propagate(blobStorageException(code, statusCode));

        assertInstanceOf(expectedType, actual);
        assertEquals(statusCode, ((UbsaException) actual).getStatusCode());
    }

    private BlobStorageException blobStorageException(String code, int statusCode) {
        return new BlobStorageException("azure " + code, new StubHttpResponse(statusCode, code), null);
    }

    private static final class StubHttpResponse extends HttpResponse {
        private final int statusCode;
        private final HttpHeaders headers;

        private StubHttpResponse(int statusCode, String errorCode) {
            super(new HttpRequest(HttpMethod.GET, "https://example.test/blob"));
            this.statusCode = statusCode;
            this.headers = new HttpHeaders().set("x-ms-error-code", errorCode);
        }

        @Override
        public int getStatusCode() {
            return statusCode;
        }

        @Override
        public String getHeaderValue(String name) {
            return null;
        }

        @Override
        public HttpHeaders getHeaders() {
            return headers;
        }

        @Override
        public Flux<ByteBuffer> getBody() {
            return Flux.empty();
        }

        @Override
        public Mono<byte[]> getBodyAsByteArray() {
            return Mono.just(new byte[0]);
        }

        @Override
        public Mono<String> getBodyAsString() {
            return Mono.just("");
        }

        @Override
        public Mono<String> getBodyAsString(Charset charset) {
            return Mono.just("");
        }
    }
}
