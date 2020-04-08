package nl.junglecomputing.pidgin;

public class DuplicateCommunicatorException extends CommunicatorException {

    private static final long serialVersionUID = -793088539831172241L;

    public DuplicateCommunicatorException(String message) {
        super(message);
    }

    public DuplicateCommunicatorException(String message, Throwable e) {
        super(message, e);
    }
}
