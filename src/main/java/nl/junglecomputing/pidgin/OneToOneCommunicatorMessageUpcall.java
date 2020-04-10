/*
 * Copyright 2020 Netherlands eScience Center
 *                Vrije Universiteit Amsterdam
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.junglecomputing.pidgin;

import java.io.IOException;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.MessageUpcall;
import ibis.ipl.ReceivePort;
import ibis.ipl.ReceivePortConnectUpcall;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

public class OneToOneCommunicatorMessageUpcall extends OneToOneCommunicator implements ReceivePortConnectUpcall {

    private final ReceivePort rp;
    private final SendPort sp;

    public OneToOneCommunicatorMessageUpcall(String name, Ibis ibis, MessageUpcall upcall) throws CommunicatorException, IOException {
        this(name, ibis, ibis.registry().joinedIbises(), upcall);
    }

    public OneToOneCommunicatorMessageUpcall(String name, Ibis ibis, IbisIdentifier[] participants, MessageUpcall upcall)
            throws CommunicatorException, IOException {
        super(name, ibis, participants);

        if (participants.length != 2) {
            throw new InvalidParticipantsException("A OneToOneCommunicatorUpcall requires 2 participants, not " + participants.length);
        }

        rp = ibis.createReceivePort(Pidgin.portTypeOneToOneUpcall, "RP_O2OU_" + name + "_" + other, upcall, this, null);
        sp = ibis.createSendPort(Pidgin.portTypeOneToOneUpcall);
    }

    @Override
    public void activate() throws IOException {

        // enable connection to the receiveport
        rp.enableConnections();

        // Connect to the other side
        sp.connect(other, "RP_O2OU_" + name + "_" + me, Pidgin.CONNECT_TIMEOUT, true);

        // Wait until the other side is connected to us.
        waitUntilReceivePortConnected(Pidgin.CONNECT_TIMEOUT);

        // disable connection to the receiveport
        rp.disableConnections();

        // enable message upcalls
        rp.enableMessageUpcalls();
    }

    @Override
    public void deactivate() throws IOException {
        rp.disableMessageUpcalls();
        sp.close();
        rp.close(Pidgin.CONNECT_TIMEOUT);
    }

    public WriteMessage send() throws IOException {
        return sp.newMessage();
    }
}
