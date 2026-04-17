package io.github.michaelcirkl.ubsa.client.exception.types;

import io.github.michaelcirkl.ubsa.client.exception.UbsaException;

/**
 * Thrown when the request could not be authenticated.
 *
 * <p>UBSA maps the following provider error codes to this exception:
 * <ul>
 *   <li>Azure: {@code AuthenticationFailed}, {@code InvalidAuthenticationInfo},
 *   {@code NoAuthenticationInformation}</li>
 *   <li>AWS: {@code InvalidToken}, {@code ExpiredToken}</li>
 * </ul>
 */
public class AuthenticationFailedException extends UbsaException {
    public AuthenticationFailedException(String message, Throwable nativeException, int statusCode) {
        super(message, nativeException, statusCode);
    }
}
