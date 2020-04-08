package nl.junglecomputing.pidgin;

public class NoParticipantsException extends CommunicatorException {

    private static final long serialVersionUID = -8651325737680234969L;

    public NoParticipantsException(String message) {
        super(message);
    }

    public NoParticipantsException(String message, Throwable e) {
        super(message, e);
    }
}
