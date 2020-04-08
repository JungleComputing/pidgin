package nl.junglecomputing.pidgin;

public class CommunicatorException extends Exception {

    private static final long serialVersionUID = 1200684321525692967L;

    public CommunicatorException(String message) {
        super(message);
    }

    public CommunicatorException(String message, Throwable e) {
        super(message, e);
    }
}
