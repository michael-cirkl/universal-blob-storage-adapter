package io.github.michaelcirkl.ubsa.client.exception.types;

import io.github.michaelcirkl.ubsa.client.exception.UbsaException;

/**
 * Thrown when the request is authenticated but not authorized to access the target resource.
 *
 * <p>UBSA maps the following provider error codes to this exception:
 * <ul>
 *   <li>Azure: {@code AuthorizationFailure}, {@code AuthorizationPermissionMismatch},
 *   {@code AuthorizationProtocolMismatch}, {@code AuthorizationResourceTypeMismatch},
 *   {@code AuthorizationServiceMismatch}, {@code AuthorizationSourceIPMismatch}</li>
 *   <li>AWS: {@code AccessDenied}</li>
 * </ul>
 */
public class AccessDeniedException extends UbsaException {
    public AccessDeniedException(String message, Throwable nativeException, int statusCode) {
        super(message, nativeException, statusCode);
    }
}
