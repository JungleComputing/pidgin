package nl.junglecomputing.pidgin;

import java.io.IOException;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.MessageUpcall;
import ibis.ipl.ReceivePort;
import ibis.ipl.WriteMessage;

public class MessageUpcallCommunicator extends Communicator {

    protected ReceivePort rports[];

    protected final MessageUpcall upcall;

    protected MessageUpcallCommunicator(String name, Ibis ibis, IbisIdentifier[] participants, MessageUpcall upcall) throws CommunicatorException {
        super(name, ibis, participants);

        this.upcall = upcall;
    }

    public WriteMessage sendMessage(IbisIdentifier dest) throws IOException {
        return null;
    }

    @Override
    public void activate() throws IOException {

    }

    @Override
    public void deactivate() throws IOException {

    }
}
