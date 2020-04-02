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
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import nl.junglecomputing.pidgin.ExplicitChannel;
import nl.junglecomputing.pidgin.NoSuchSourceException;

public class ExplicitChannelImpl extends ChannelImpl implements ExplicitChannel {

    private static final Logger logger = LoggerFactory.getLogger(ExplicitChannelImpl.class);

    private ReceivePort rports[];

    private final ConcurrentHashMap<IbisIdentifier, ReceivePort> receiveports = new ConcurrentHashMap<IbisIdentifier, ReceivePort>();

    public ExplicitChannelImpl(Ibis ibis, String name, IbisIdentifier[] ids) throws IOException {
        super(ibis, name);

        System.err.println("Creating ClosedExplicitChannel " + name);

        rports = new ReceivePort[ids.length];

        System.err.println("ClosedExplicitChannel " + name + " has " + ids.length + " members");

        for (int i = 0; i < rports.length; i++) {
            if (!ids[i].equals(ibis.identifier())) {
                rports[i] = ibis.createReceivePort(getPortType(), getReceivePortName(ids[i]));
                rports[i].enableConnections();

                receiveports.put(ids[i], rports[i]);

                System.err.println("ClosedExplicitChannel created RP " + getReceivePortName(ids[i]));
            }
        }
    }

    @Override
    public void activate() throws IOException {
        setActive(true);
    }

    @Override
    protected String getReceivePortName(IbisIdentifier id) {
        return name + "_" + id.name();
    }

    @Override
    protected void disableReceivePorts() {
        for (ReceivePort rp : rports) {
            if (rp != null) {
                rp.disableConnections();
            }
        }
    }

    @Override
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

    @Override
    protected PortType getPortType() {
        return PidginImpl.portTypeOneToOneExplicit;
    }

    @Override
    public ReadMessage receiveMessage(IbisIdentifier source, long timeout) throws IOException {

        ReceivePort rp = receiveports.get(source);

        if (rp == null) {
            throw new NoSuchSourceException("No such source: " + source);
        }

        return rp.receive(timeout);
    }
}
