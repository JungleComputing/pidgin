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
import ibis.ipl.ReceivePort;
import ibis.ipl.ReceivePortConnectUpcall;
import ibis.ipl.SendPortIdentifier;
import ibis.ipl.WriteMessage;

public abstract class OneToOneCommunicator extends Communicator implements ReceivePortConnectUpcall {

    private boolean connected = false;

    protected final IbisIdentifier other;

    protected OneToOneCommunicator(String name, Ibis ibis, IbisIdentifier[] participants) throws CommunicatorException, IOException {
        super(name, ibis, participants);

        if (participants.length != 2) {
            throw new InvalidParticipantsException("A OneToOneCommunicatorExplicit requires 2 participants, not " + participants.length);
        }

        other = participants[(rank() + 1) % 2];
    }

    protected synchronized void waitUntilReceivePortConnected(long timeout) throws IOException {

        long now = System.currentTimeMillis();
        long deadline = now + timeout;

        while (!connected && now < deadline) {

            try {
                wait(deadline - now);
            } catch (InterruptedException e) {
                throw new IOException("ReceivePort interrupted while waiting for a connection!");
            }

            now = System.currentTimeMillis();
        }

        if (!connected) {
            throw new IOException("ReceivePort never got a connection!");
        }
    }

    private synchronized void setReceivePortConnected() {
        connected = true;
        notifyAll();
    }

    @Override
    public boolean gotConnection(ReceivePort receiver, SendPortIdentifier applicant) {

        if (other.equals(applicant.ibisIdentifier())) {
            // We got an incoming connection request from the other side
            setReceivePortConnected();
            return true;
        }

        // Some impostor is trying to connect!
        return false;
    }

    @Override
    public void lostConnection(ReceivePort receiver, SendPortIdentifier origin, Throwable cause) {
        // TODO Auto-generated method stub

    }

    public abstract WriteMessage send() throws IOException;
}
