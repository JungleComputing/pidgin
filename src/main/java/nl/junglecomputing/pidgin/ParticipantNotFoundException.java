package nl.junglecomputing.pidgin;

public class ParticipantNotFoundException extends CommunicatorException {

    private static final long serialVersionUID = -3660023944823761777L;

    public ParticipantNotFoundException(String message) {
        super(message);
    }

    public ParticipantNotFoundException(String message, Throwable e) {
        super(message, e);
    }
}
