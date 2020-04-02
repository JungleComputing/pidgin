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
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.MessageUpcall;
import ibis.ipl.PortType;
import ibis.ipl.ReceivePort;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;
import nl.junglecomputing.pidgin.Channel;
import nl.junglecomputing.pidgin.ChannelNotActiveException;

public abstract class ChannelImpl implements Channel {

    private static final Logger logger = LoggerFactory.getLogger(ChannelImpl.class);

    private static final long CONNECT_TIMEOUT = 60 * 1000;

    protected final Ibis ibis;

    protected final String name;

    private boolean active = false;

    protected ReceivePort rports[];

    private final ConcurrentHashMap<IbisIdentifier, SendPort> sendports = new ConcurrentHashMap<IbisIdentifier, SendPort>();

    protected final ConcurrentHashMap<IbisIdentifier, ReceivePort> receiveports = new ConcurrentHashMap<IbisIdentifier, ReceivePort>();

    private ChannelImpl(Ibis ibis, String name, IbisIdentifier[] ids, boolean hasUpcall, MessageUpcall upcall) throws IOException {
        this.ibis = ibis;
        this.name = name;

        if (logger.isInfoEnabled()) {
            logger.info("Creating ChannelImpl " + name);
        }

        rports = new ReceivePort[ids.length];

        if (logger.isInfoEnabled()) {
            logger.info("ChannelImpl " + name + " has " + ids.length + " members");
        }

        for (int i = 0; i < rports.length; i++) {
            if (!ids[i].equals(ibis.identifier())) {
                if (hasUpcall) {
                    rports[i] = ibis.createReceivePort(getPortType(), getReceivePortName(ids[i]), upcall);
                } else {
                    rports[i] = ibis.createReceivePort(getPortType(), getReceivePortName(ids[i]));
                }

                rports[i].enableConnections();

                receiveports.put(ids[i], rports[i]);

                if (logger.isInfoEnabled()) {
                    logger.info("ChannelImpl created RP " + getReceivePortName(ids[i]));
                }
            }
        }
    }

    protected ChannelImpl(Ibis ibis, String name, IbisIdentifier[] ids, MessageUpcall upcall) throws IOException {
        this(ibis, name, ids, true, upcall);
    }

    protected ChannelImpl(Ibis ibis, String name, IbisIdentifier[] ids) throws IOException {
        this(ibis, name, ids, false, null);
    }

    protected final String getName() {
        return name;
    }

    protected final synchronized boolean setActive(boolean value) {
        boolean old = active;
        active = value;
        return old;
    }

    protected final synchronized boolean isActive() {
        return active;
    }

    public abstract void activate() throws IOException;

    private void closeSendPorts() {

        logger.debug("Closing sendports");

        for (SendPort sp : sendports.values()) {
            try {
                sp.close();
            } catch (Exception e) {
                logger.info("Close sendport got exception", e);
            }
        }

        logger.debug("All sendports closed");
    }

    @Override
    public void deactivate() throws IOException {

        boolean wasActive = setActive(false);

        if (!wasActive) {
            return;
        }

        disableReceivePorts();

        closeSendPorts();

        closeReceivePorts();
    }

    protected abstract String getReceivePortName(IbisIdentifier id);

    protected abstract PortType getPortType();

    protected void disableReceivePorts() {
        for (ReceivePort rp : rports) {
            if (rp != null) {
                rp.disableConnections();
            }
        }
    }

    protected void closeReceivePorts() {
        if (rports != null) {
            for (ReceivePort rport : rports) {
                if (rport != null) {
                    try {
                        rport.close(10000);
                    } catch (IOException e) {
                        logger.info("Close receive port " + rport.name() + " got exception", e);
                    }
                }
            }
        }
    }

    private SendPort createAndConnect(IbisIdentifier id, String rpName, long timeout) throws IOException {

        if (logger.isInfoEnabled()) {
            logger.info("Connecting to " + id.name() + ":" + rpName + " from " + ibis.identifier());
        }

        SendPort sp = null;

        try {
            sp = ibis.createSendPort(getPortType());
            sp.connect(id, rpName, timeout, true);

            if (logger.isInfoEnabled()) {
                logger.info("Connecting to " + id.name() + ":" + rpName + " from " + ibis.identifier());
            }

        } catch (IOException e) {
            try {
                sp.close();
            } catch (Throwable e2) {
                // ignored ?
            }
            logger.error("Could not connect to " + id.name() + ":" + rpName, e);
            throw e;
        }
        return sp;
    }

    private SendPort getSendPort(IbisIdentifier id) throws IOException {

        if (id.equals(ibis.identifier())) {
            logger.error("Sending to myself! " + id + " " + ibis.identifier(), new Throwable());
        }

        SendPort sp = sendports.get(id);

        if (sp == null) {
            String rpName = getReceivePortName(ibis.identifier());

            sp = createAndConnect(id, rpName, CONNECT_TIMEOUT);

            if (logger.isInfoEnabled()) {
                logger.info("Succesfully connected to " + id + ":" + rpName + " from " + ibis.identifier());
            }

            SendPort sp2 = sendports.putIfAbsent(id, sp);

            if (sp2 != null) {
                // Someone managed to sneak in between our get and put!
                try {
                    sp.close();
                } catch (Exception e) {
                    // ignored
                }

                sp = sp2;
            }
        }

        return sp;
    }

    public WriteMessage sendMessage(IbisIdentifier destination) throws IOException {

        if (!isActive()) {
            throw new ChannelNotActiveException("Cannot send message, channel " + name + " not active");
        }

        try {
            return getSendPort(destination).newMessage();
        } catch (IOException e) {
            logger.warn("Failed to connect to " + destination, e);
            throw e;
        }
    }
}
