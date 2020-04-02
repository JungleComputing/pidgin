package nl.junglecomputing.pidgin;

import java.io.IOException;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.WriteMessage;

public interface UpcallChannel extends Channel {

    public WriteMessage sendMessage(IbisIdentifier dest) throws IOException;
}
