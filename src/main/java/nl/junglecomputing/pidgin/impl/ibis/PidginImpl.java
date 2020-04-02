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
package nl.junglecomputing.pidgin.impl.ibis;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.MessageUpcall;
import ibis.ipl.PortType;
import nl.junglecomputing.pidgin.DuplicateChannelException;
import nl.junglecomputing.pidgin.ExplicitChannel;
import nl.junglecomputing.pidgin.MessageUpcallChannel;
import nl.junglecomputing.pidgin.Pidgin;

public class PidginImpl implements Pidgin {

    private static final Logger logger = LoggerFactory.getLogger(PidginImpl.class);

    protected static final PortType portTypeManyToOneUpcall = new PortType(PortType.COMMUNICATION_FIFO, PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_OBJECT, PortType.RECEIVE_AUTO_UPCALLS, PortType.RECEIVE_TIMEOUT, PortType.CONNECTION_MANY_TO_ONE);

    protected static final PortType portTypeOneToOneUpcall = new PortType(PortType.COMMUNICATION_FIFO, PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_OBJECT, PortType.RECEIVE_AUTO_UPCALLS, PortType.RECEIVE_TIMEOUT, PortType.CONNECTION_ONE_TO_ONE);

    protected static final PortType portTypeOneToOneExplicit = new PortType(PortType.COMMUNICATION_FIFO, PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_OBJECT, PortType.RECEIVE_EXPLICIT, PortType.RECEIVE_TIMEOUT, PortType.RECEIVE_TIMEOUT, PortType.CONNECTION_ONE_TO_ONE);

    private static final IbisCapabilities closedIbisCapabilities = new IbisCapabilities(IbisCapabilities.CLOSED_WORLD, IbisCapabilities.TERMINATION,
            IbisCapabilities.ELECTIONS_STRICT, IbisCapabilities.MEMBERSHIP_TOTALLY_ORDERED);

    private final Ibis ibis;

    private final IbisIdentifier master;

    private final IbisIdentifier local;

    private final HashSet<String> channels = new HashSet<>();

    private int rank = -1;

    private final boolean isMaster;

    private final IbisIdentifier[] ids;

    public PidginImpl(final Properties properties) throws Exception {

        ibis = IbisFactory.createIbis(closedIbisCapabilities, properties, true, null, portTypeManyToOneUpcall, portTypeOneToOneUpcall,
                portTypeOneToOneExplicit);

        ibis.registry().waitUntilPoolClosed();
        ids = ibis.registry().joinedIbises();

        master = ibis.registry().elect("Pidgin Master");

        local = ibis.identifier();

        isMaster = master.equals(local);

        // We determine our rank here. This rank should only be used for debugging purposes!
        for (int i = 0; i < ids.length; i++) {
            if (ids[i].equals(local)) {
                rank = i;
                break;
            }
        }
    }

    @Override
    public Ibis getIbis() {
        return ibis;
    }

    @Override
    public IbisIdentifier getMaster() {
        return master;
    }

    @Override
    public IbisIdentifier getMyIdentifier() {
        return local;
    }

    @Override
    public boolean isMaster() {
        return isMaster;
    }

    @Override
    public int getPoolSize() {
        return ibis.registry().getPoolSize();
    }

    public void terminate() throws IOException {

        // for (Channel c : channels.values()) {
        // c.deactivate();
        // }

        if (local.equals(master)) {
            ibis.registry().terminate();
        } else {
            ibis.registry().waitUntilTerminated();
        }
    }

    @Override
    public int getRank() {
        return rank;
    }

    @Override
    public IbisIdentifier getElectionResult(String electTag, long timeout) throws IOException {
        return ibis.registry().getElectionResult(electTag, timeout);
    }

    @Override
    public IbisIdentifier elect(String electTag) throws IOException {
        return ibis.registry().elect(electTag);
    }

    @Override
    public IbisIdentifier[] getAllIdentifiers() {
        return ids;
    }

    // public Channel getChannel(String name) throws NoSuchChannelException {
    //
    // synchronized (channels) {
    // PidginChannel c = channels.get(name);
    //
    // if (c == null) {
    // throw new NoSuchChannelException("Channel " + name + " not found!");
    // }
    //
    // return c;
    // }
    // }

    @Override
    public MessageUpcallChannel createUpcallChannel(String name, IbisIdentifier[] praticipants, MessageUpcall upcall)
            throws DuplicateChannelException, IOException {

        logger.info("Creating MessageUpcallChannel " + name);

        synchronized (channels) {
            if (channels.contains(name)) {
                throw new DuplicateChannelException("Channel already exists " + name);
            }

            channels.add(name);
        }

        return new MessageUpcallChannelImpl(ibis, name, upcall, ids);
    }

    @Override
    public ExplicitChannel createExplicitChannel(String name, IbisIdentifier[] praticipants) throws DuplicateChannelException, IOException {

        logger.info("Creating ExplicitChannel " + name);

        synchronized (channels) {
            if (channels.contains(name)) {
                throw new DuplicateChannelException("Channel already exists " + name);
            }

            channels.add(name);
        }

        return new ExplicitChannelImpl(ibis, name, ids);
    }

    // @Override
    // public void activateChannel(String name) throws IOException, NoSuchChannelException {
    //
    // System.err.println("Activating channel " + name);
    //
    // getChannel(name).activate();
    // }

}
