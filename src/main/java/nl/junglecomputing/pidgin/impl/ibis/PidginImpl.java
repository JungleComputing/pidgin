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
import java.util.HashMap;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;
import ibis.ipl.RegistryEventHandler;
import nl.junglecomputing.pidgin.Channel;
import nl.junglecomputing.pidgin.DuplicateChannelException;
import nl.junglecomputing.pidgin.NoSuchChannelException;
import nl.junglecomputing.pidgin.NodeIdentifier;
import nl.junglecomputing.pidgin.Pidgin;
import nl.junglecomputing.pidgin.Upcall;

public class PidginImpl implements Pidgin, RegistryEventHandler {

    private static final Logger logger = LoggerFactory.getLogger(PidginImpl.class);

    protected static final PortType portTypeManyToOne = new PortType(PortType.COMMUNICATION_FIFO, PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_OBJECT, PortType.RECEIVE_AUTO_UPCALLS, PortType.RECEIVE_TIMEOUT, PortType.CONNECTION_MANY_TO_ONE);

    protected static final PortType portTypeOneToOne = new PortType(PortType.COMMUNICATION_FIFO, PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_OBJECT,
            PortType.RECEIVE_AUTO_UPCALLS, PortType.RECEIVE_TIMEOUT, PortType.CONNECTION_ONE_TO_ONE);

    private static final IbisCapabilities openIbisCapabilities = new IbisCapabilities(IbisCapabilities.MALLEABLE, IbisCapabilities.TERMINATION,
            IbisCapabilities.ELECTIONS_STRICT, IbisCapabilities.MEMBERSHIP_TOTALLY_ORDERED);

    private static final IbisCapabilities closedIbisCapabilities = new IbisCapabilities(IbisCapabilities.CLOSED_WORLD, IbisCapabilities.TERMINATION,
            IbisCapabilities.ELECTIONS_STRICT, IbisCapabilities.MEMBERSHIP_TOTALLY_ORDERED);

    private Ibis ibis;

    private final IbisIdentifier master;

    private final IbisIdentifier local;

    private final HashMap<String, PidginChannel> channels = new HashMap<>();

    private int rank = -1;

    private final boolean closedPool;

    private final boolean isMaster;

    private IbisIdentifier[] ids = null;

    private final Properties properties;

    // private TimerImpl communicationTimer;
    //
    // private final Profiling profiling;

    public PidginImpl(final Properties properties) throws Exception {

        closedPool = true; // properties.CLOSED;
        this.properties = properties;

        ibis = IbisFactory.createIbis(closedPool ? closedIbisCapabilities : openIbisCapabilities, properties, true, closedPool ? null : this, portTypeManyToOne,
                portTypeOneToOne);

        if (closedPool) {
            ibis.registry().waitUntilPoolClosed();
            ids = ibis.registry().joinedIbises();
        } else {
            ibis.registry().enableEvents();
        }

        boolean canBeMaster = true; // properties.MASTER;

        if (canBeMaster) {
            // Elect a server
            master = ibis.registry().elect("Pidgin Master");
        } else {
            master = ibis.registry().getElectionResult("Pidgin Master");
        }

        local = ibis.identifier();

        isMaster = master.equals(local);

        // We determine our rank here. This rank should only be used for debugging purposes!
        if (closedPool) {
            for (int i = 0; i < ids.length; i++) {
                if (ids[i].equals(local)) {
                    rank = i;
                    break;
                }
            }
        }

        if (rank == -1) {
            rank = (int) ibis.registry().getSequenceNumber("pidgin-" + master.toString());
        }

        // String tmp = properties.getProperty(ConstellationProperties.S_PREFIX + "rank");

        // if (tmp != null) {
        // try {
        // rank = Integer.parseInt(tmp);
        // } catch (Exception e) {
        // logger.error("Failed to parse rank: " + tmp);
        // }
        // }
        //
        // if (rank == -1) {
        // rank = (int) ibis.registry().getSequenceNumber("pidgin-" + master.toString());
        // }
    }

    @Override
    public NodeIdentifier getMaster() {
        return new NodeIdentifierImpl(master);
    }

    @Override
    public NodeIdentifier getMyIdentifier() {
        return new NodeIdentifierImpl(local);
    }

    @Override
    public int getPoolSize() {
        return ibis.registry().getPoolSize();
    }

    @Override
    public void terminate() throws IOException {

        if (local.equals(master)) {
            ibis.registry().terminate();
        } else {
            ibis.registry().waitUntilTerminated();
        }
    }

    // @Override
    // public void cleanup() {
    // synchronized (channels) {
    //
    // Set<String> keys = channels.keySet();
    //
    // for (String k : keys) {
    //
    // PidginChannel c = channels.remove(k);
    //
    // if (c != null) {
    // try {
    // c.cleanup();
    // } catch (Exception e) {
    // logger.warn("Failed to cleanup channel " + k, e);
    // }
    // }
    // }
    // }
    // }

    // @Override
    // public void cleanup(NodeIdentifier id) {
    // IbisIdentifier dest = ((NodeIdentifierImpl) id).getIbisIdentifier();
    //
    // synchronized (channels) {
    // for (PidginChannel c : channels.values()) {
    // try {
    // c.cleanup(dest);
    // } catch (Exception e) {
    // logger.warn("Failed to cleanup node " + id + " from channel " + c.getName(), e);
    // }
    // }
    // }
    // }
    //
    // @Override
    // public boolean sendMessage(String channel, NodeIdentifier dest, byte opcode, Object data, ByteBuffer... buffers) throws NoSuchChannelException {
    // return getChannel(channel).sendMessage(dest, opcode, data, buffers);
    // }

    @Override
    public int getRank() {
        return rank;
    }

    @Override
    public void died(IbisIdentifier id) {
        left(id);
    }

    @Override
    public void electionResult(String arg0, IbisIdentifier arg1) {
        // ignored

    }

    @Override
    public void gotSignal(String arg0, IbisIdentifier arg1) {
        // ignored

    }

    @Override
    public void joined(IbisIdentifier arg0) {
        // ignored
    }

    @Override
    public void left(IbisIdentifier arg0) {
        // ignored
    }

    @Override
    public void poolClosed() {
        // ignored

    }

    @Override
    public void poolTerminated(IbisIdentifier arg0) {
        // ignored

    }

    // @Override
    // public void activate() throws IOException {
    //
    // // if (properties.PROFILE_COMMUNICATION) {
    // // communicationTimer = pool.getProfiling().getTimer("java", "data handling", "read/write data");
    // // } else {
    // // communicationTimer = null;
    // // }
    //
    // for (PidginChannel c : channels.values()) {
    // c.activate();
    // }
    // }

    @Override
    public NodeIdentifier getElectionResult(String electTag, long timeout) throws IOException {
        IbisIdentifier id = ibis.registry().getElectionResult(electTag, timeout);
        if (id != null) {
            return new NodeIdentifierImpl(id);
        }
        return null;
    }

    @Override
    public NodeIdentifier elect(String electTag) throws IOException {
        return new NodeIdentifierImpl(ibis.registry().elect(electTag));
    }

    @Override
    public NodeIdentifier[] getNodeIdentifiers() {
        if (!closedPool) {
            return null;
        }
        NodeIdentifier[] result = new NodeIdentifier[ids.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = new NodeIdentifierImpl(ids[i]);
        }
        return result;
    }

    public Channel getChannel(String name) throws NoSuchChannelException {

        synchronized (channels) {
            PidginChannel c = channels.get(name);

            if (c == null) {
                throw new NoSuchChannelException("Channel " + name + " not found!");
            }

            return c;
        }
    }

    @Override
    public Channel createChannel(String name, Upcall upcall) throws DuplicateChannelException, IOException {

        System.err.println("Creating channel " + name);

        PidginChannel p = null;

        synchronized (channels) {
            if (channels.containsKey(name)) {
                throw new DuplicateChannelException("Channel already exists " + name);
            }

            if (closedPool) {
                p = new ClosedPidginChannel(ibis, name, upcall, ids);
            } else {
                p = new OpenPidginChannel(ibis, name, upcall);
            }

            channels.put(name, p);
        }

        System.err.println("Created channel " + name);

        return p;
    }

    // @Override
    // public void activateChannel(String name) throws IOException, NoSuchChannelException {
    //
    // System.err.println("Activating channel " + name);
    //
    // getChannel(name).activate();
    // }

    @Override
    public void removeChannel(String name) throws IOException {

        PidginChannel c = null;

        synchronized (channels) {
            c = channels.remove(name);
        }

        // c.deactivate();
    }

    @Override
    public boolean isMaster() {
        return isMaster;
    }
}
