package michaelcirkl.ubsa;

public class UbsaException extends RuntimeException {
    private int statusCode; //make final later
    public UbsaException(String message, Throwable nativeException) {
        super(message, nativeException);
    }

    public UbsaException(String message, Throwable nativeException, int statusCode) {
        super(message, nativeException);
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }
}
