package michaelcirkl.ubsa.client.exception.types;

import michaelcirkl.ubsa.client.exception.UbsaException;

/**
 * Thrown when the requested blob/object does not exist.
 *
 * <p>UBSA maps the following provider error codes to this exception:
 * <ul>
 *   <li>Azure: {@code BlobNotFound}</li>
 *   <li>AWS: {@code NoSuchKey}</li>
 * </ul>
 */
public class BlobNotFoundException extends UbsaException {
    public BlobNotFoundException(String message, Throwable nativeException, int statusCode) {
        super(message, nativeException, statusCode);
    }
}
