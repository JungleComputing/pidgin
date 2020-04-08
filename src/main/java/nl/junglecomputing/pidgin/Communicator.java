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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;

public abstract class Communicator {

    private static final Logger logger = LoggerFactory.getLogger(Communicator.class);

    protected static final PortType portTypeManyToOneUpcall = new PortType(PortType.COMMUNICATION_FIFO, PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_OBJECT, PortType.RECEIVE_AUTO_UPCALLS, PortType.RECEIVE_TIMEOUT, PortType.CONNECTION_MANY_TO_ONE);

    protected static final PortType portTypeOneToOneUpcall = new PortType(PortType.COMMUNICATION_FIFO, PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_OBJECT, PortType.RECEIVE_AUTO_UPCALLS, PortType.RECEIVE_TIMEOUT, PortType.CONNECTION_ONE_TO_ONE);

    protected static final PortType portTypeOneToOneExplicit = new PortType(PortType.COMMUNICATION_FIFO, PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_OBJECT, PortType.RECEIVE_EXPLICIT, PortType.RECEIVE_TIMEOUT, PortType.RECEIVE_TIMEOUT, PortType.CONNECTION_ONE_TO_ONE);

    public static final long ELECT_TIMEOUT = 1000L;

    protected final IbisIdentifier[] participants;
    protected final Ibis ibis;
    protected final IbisIdentifier me;
    protected final int rank;
    protected final String name;

    protected Communicator(String name, Ibis ibis, IbisIdentifier[] participants) throws CommunicatorException {

        if (name == null || name.isEmpty()) {
            throw new CommunicatorException("Communicator has invalid name: " + name);
        }

        if (ibis == null) {
            throw new CommunicatorException("Ibis is null");
        }

        if (participants == null || participants.length == 0) {
            throw new NoParticipantsException("Participants is null or empty: " + participants);
        }

        this.participants = participants.clone();
        this.ibis = ibis;
        this.me = ibis.identifier();
        this.name = name;

        // We determine our rank here and check if the participant array contains null values.
        int tmp = -1;

        for (int i = 0; i < this.participants.length; i++) {

            IbisIdentifier id = this.participants[i];

            if (id == null) {
                throw new NullPointerException("Participant[ " + i + "] is null");
            }

            if (me.equals(id)) {

                if (tmp != -1) {
                    throw new DuplicateParticiantException("Participant[ " + i + "] is duplicate of " + tmp);
                }

                tmp = i;
            }
        }

        if (tmp == -1) {
            throw new ParticipantNotFoundException("Cannot find this ibis in participants");
        }

        rank = tmp;

        if (rank == 0) {
            IbisIdentifier master = null;

            try {
                master = ibis.registry().elect(name, ELECT_TIMEOUT);
            } catch (Exception e) {
                throw new CommunicatorException("Failed to verify name of communicator: " + name, e);
            }

            if (master == null) {
                throw new CommunicatorException("Failed to verify name of communicator: " + name);
            }

            if (!me.equals(master)) {
                throw new DuplicateCommunicatorException("Communicator already exist: " + name);
            }
        }
    }

    public int rank() {
        return rank;
    }

    public int size() {
        return participants.length;
    }

    protected synchronized boolean allowedSender(IbisIdentifier iid, String name) {

        if (!this.name.equals(name)) {
            logger.warn("Communicator " + this.name + " rejection connection from: " + iid + "/" + name);
            return false;
        }

        // Check for connection to self
        if (me.equals(iid)) {
            logger.warn("Communicator " + this.name + " rejection connection self");
            return false;
        }

        // Check all participants to see if the participant is present.
        for (int i = 0; i < participants.length; i++) {
            if (iid.equals(participants[i])) {
                if (logger.isInfoEnabled()) {
                    logger.info("Communicator " + this.name + " accepting connection from participant: " + iid + "/" + name);
                }
                return true;
            }
        }

        // If we fall thru the participant is not known.
        logger.warn("Communicator " + this.name + " rejecting connection from unknown participant: " + iid + "/" + name);
        return false;
    }

    public abstract void activate() throws IOException;

    public abstract void deactivate() throws IOException;

    protected abstract String getReceivePortName(IbisIdentifier sender);

    protected abstract PortType getPortType();

}
