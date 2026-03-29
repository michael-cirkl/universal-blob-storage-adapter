package michaelcirkl.ubsa.client.exception.types;

import michaelcirkl.ubsa.client.exception.UbsaException;

/**
 * Thrown when the requested bucket/container does not exist.
 *
 * <p>UBSA maps the following provider error codes to this exception:
 * <ul>
 *   <li>Azure: {@code ContainerNotFound}</li>
 *   <li>AWS: {@code NoSuchBucket}</li>
 * </ul>
 */
public class BucketNotFoundException extends UbsaException {
    public BucketNotFoundException(String message, Throwable nativeException, int statusCode) {
        super(message, nativeException, statusCode);
    }
}
