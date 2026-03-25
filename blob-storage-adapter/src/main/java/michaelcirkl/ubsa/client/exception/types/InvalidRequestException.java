package michaelcirkl.ubsa.client.exception.types;

import michaelcirkl.ubsa.client.exception.UbsaException;

public class InvalidRequestException extends UbsaException {
    public InvalidRequestException(String message, Throwable nativeException, int statusCode) {
        super(message, nativeException, statusCode);
    }
}
