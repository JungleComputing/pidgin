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
import ibis.ipl.WriteMessage;

public class AllToAllCommunicatorMessageUpcall extends Communicator {

    private final OneToOneCommunicatorMessageUpcall[] communicators;

    public AllToAllCommunicatorMessageUpcall(String name, Ibis ibis, MessageUpcall upcall) throws CommunicatorException, IOException {
        this(name, ibis, ibis.registry().joinedIbises(), upcall);
    }

    public AllToAllCommunicatorMessageUpcall(String name, Ibis ibis, IbisIdentifier[] participants, MessageUpcall upcall)
            throws CommunicatorException, IOException {
        super(name, ibis, participants);

        communicators = new OneToOneCommunicatorMessageUpcall[participants.length];

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

                communicators[i] = new OneToOneCommunicatorMessageUpcall(comm_name, ibis, p, upcall);
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

}
