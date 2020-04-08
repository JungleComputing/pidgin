package nl.junglecomputing.pidgin;

public class DuplicateParticiantException extends CommunicatorException {

    private static final long serialVersionUID = 6083839685310771215L;

    public DuplicateParticiantException(String message) {
        super(message);
    }

    public DuplicateParticiantException(String message, Throwable e) {
        super(message, e);
    }
}
