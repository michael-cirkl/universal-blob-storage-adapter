package io.github.michaelcirkl.ubsa.client.exception;

/**
 * Base runtime exception for failures reported by the UBSA clients.
 *
 * <p>Provider-specific SDK exceptions are wrapped in this type so callers can handle failures through a
 * provider-neutral API. When UBSA can classify the failure more precisely, it throws a subtype such as
 * {@code BucketNotFoundException} or {@code AccessDeniedException}; otherwise this base type is used.
 *
 * <p>Available diagnostic information includes the inherited exception message, the original provider exception
 * exposed through {@link #getCause()}, and the provider status code when one was available.
 */
public class UbsaException extends RuntimeException {
    private Integer statusCode;

    public UbsaException(String message, Throwable nativeException) {
        super(message, nativeException);
    }

    public UbsaException(String message, Throwable nativeException, int statusCode) {
        super(message, nativeException);
        this.statusCode = statusCode;
    }

    /**
     * Returns the provider status code captured from the underlying SDK exception.
     */
    public Integer getStatusCode() {
        return statusCode;
    }
}
