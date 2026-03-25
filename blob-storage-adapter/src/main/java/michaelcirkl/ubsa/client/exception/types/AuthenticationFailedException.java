package michaelcirkl.ubsa.client.exception.types;

import michaelcirkl.ubsa.client.exception.UbsaException;

public class AuthenticationFailedException extends UbsaException {
    public AuthenticationFailedException(String message, Throwable nativeException, int statusCode) {
        super(message, nativeException, statusCode);
    }
}
