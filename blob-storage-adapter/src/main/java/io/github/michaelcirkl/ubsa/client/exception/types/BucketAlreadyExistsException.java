package io.github.michaelcirkl.ubsa.client.exception.types;

import io.github.michaelcirkl.ubsa.client.exception.UbsaException;

/**
 * Thrown when creating a bucket/container that already exists.
 *
 * <p>UBSA maps the following provider error codes to this exception:
 * <ul>
 *   <li>Azure: {@code ContainerAlreadyExists}</li>
 *   <li>AWS: {@code BucketAlreadyExists}, {@code BucketAlreadyOwnedByYou}</li>
 * </ul>
 */
public class BucketAlreadyExistsException extends UbsaException {
    public BucketAlreadyExistsException(String message, Throwable nativeException, int statusCode) {
        super(message, nativeException, statusCode);
    }
}
