package nl.junglecomputing.pidgin;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.ReceivePortConnectUpcall;
import ibis.ipl.SendPortIdentifier;
import ibis.ipl.WriteMessage;

public class ExplicitCommunicator extends Communicator implements ReceivePortConnectUpcall {

    private static final Logger logger = LoggerFactory.getLogger(ExplicitCommunicator.class);

    protected static final PortType portTypeManyToOneExplicit = new PortType(PortType.COMMUNICATION_FIFO, PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_OBJECT, PortType.RECEIVE_EXPLICIT, PortType.RECEIVE_TIMEOUT, PortType.CONNECTION_UPCALLS, PortType.CONNECTION_MANY_TO_ONE);

    protected ReceivePort rport;

    protected ExplicitCommunicator(String name, Ibis ibis, IbisIdentifier[] participants) throws CommunicatorException, IOException {
        super(name, ibis, participants);

        rport = ibis.createReceivePort(portTypeManyToOneExplicit, name, this);
        rport.enableConnections();
    }

    public WriteMessage sendMessage(IbisIdentifier dest) throws IOException {
        return null;
    }

    public ReadMessage receiveMessage(long timeout) throws IOException {
        return null;
    }

    public ReadMessage receiveMessage() throws IOException {
        return receiveMessage(0L);
    }

    @Override
    public void activate() throws IOException {

    }

    @Override
    public void deactivate() throws IOException {

    }

    @Override
    protected String getReceivePortName(IbisIdentifier sender) {

        return null;
    }

    @Override
    protected PortType getPortType() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean gotConnection(ReceivePort receiver, SendPortIdentifier applicant) {
        return allowedSender(applicant);
    }

    @Override
    public void lostConnection(ReceivePort receiver, SendPortIdentifier origin, Throwable cause) {
        logger.warn("Connection lost to: " + origin + );
        
    }
}
