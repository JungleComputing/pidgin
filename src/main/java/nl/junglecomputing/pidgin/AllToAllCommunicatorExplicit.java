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
import ibis.ipl.ReadMessage;
import ibis.ipl.WriteMessage;

public class AllToAllCommunicatorExplicit extends Communicator {

    private final OneToOneCommunicatorExplicit[] communicators;

    private int pollOffset = 0;

    public AllToAllCommunicatorExplicit(String name, Ibis ibis, IbisIdentifier[] participants) throws CommunicatorException, IOException {
        super(name, ibis, participants);

        communicators = new OneToOneCommunicatorExplicit[participants.length];

        for (int i = 0; i < size; i++) {

            if (i != rank) {

                String comm_name = null;

                IbisIdentifier[] p = new IbisIdentifier[2];

                if (i < rank) {
                    p[0] = participants[i];
                    p[1] = me;
                    comm_name = name + "_" + i + "_" + rank;
                } else {
                    p[0] = me;
                    p[1] = participants[i];
                    comm_name = name + "_" + rank + "_" + i;
                }

                communicators[i] = new OneToOneCommunicatorExplicit(comm_name, ibis, p);
            }
        }
    }

    @Override
    public void activate() throws IOException {
        for (int i = 0; i < size; i++) {
            if (i != rank) {
                communicators[i].activate();
            }
        }
    }

    @Override
    public void deactivate() throws IOException {
        for (int i = 0; i < size; i++) {
            if (i != rank) {
                communicators[i].deactivate();
            }
        }
    }

    private void checkRank(int rank) throws IOException {
        if (this.rank == rank) {
            throw new IOException("Cannot communicate with myself");
        }
    }

    public WriteMessage send(int destRank) throws IOException {
        checkRank(destRank);
        return communicators[destRank].send();
    }

    public ReadMessage receive(int srcRank) throws IOException {
        checkRank(srcRank);
        return communicators[srcRank].receive();
    }

    public ReadMessage receive(int srcRank, long timeout) throws IOException {
        checkRank(srcRank);
        return communicators[srcRank].receive(timeout);
    }

    private synchronized int getPollOffset() {
        return pollOffset;
    }

    private synchronized void setPollOffset(int value) {
        pollOffset = value;
    }

    public ReadMessage poll() throws IOException {

        int offset = getPollOffset();

        for (int i = 0; i < size; i++) {

            int index = (offset + i) % size;

            if (index != rank) {
                ReadMessage result = communicators[index].poll();

                if (result != null) {
                    setPollOffset(index);
                    return result;
                }
            }
        }

        // Note, if we don't receive a message, the poll offset remains unchanged.
        return null;
    }
}
