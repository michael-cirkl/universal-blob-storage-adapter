package io.github.michaelcirkl.ubsa.client.exception.types;

import io.github.michaelcirkl.ubsa.client.exception.UbsaException;

/**
 * Thrown when the provider times out while processing the request.
 *
 * <p>UBSA maps the following provider error codes to this exception:
 * <ul>
 *   <li>Azure: {@code OperationTimedOut}</li>
 *   <li>AWS: {@code RequestTimeout}</li>
 * </ul>
 */
public class RequestTimeoutException extends UbsaException {
    public RequestTimeoutException(String message, Throwable nativeException, int statusCode) {
        super(message, nativeException, statusCode);
    }
}
