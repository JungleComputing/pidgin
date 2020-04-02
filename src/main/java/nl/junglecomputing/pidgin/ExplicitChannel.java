package nl.junglecomputing.pidgin;

import java.io.IOException;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReadMessage;
import ibis.ipl.WriteMessage;

public interface ExplicitChannel extends Channel {

    public WriteMessage sendMessage(IbisIdentifier dest) throws IOException;

    public ReadMessage receiveMessage(IbisIdentifier source, long timeout) throws IOException;

    public default ReadMessage receiveMessage(IbisIdentifier source) throws IOException {
        return receiveMessage(source, 0L);
    }
}
