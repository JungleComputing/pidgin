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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.MessageUpcall;
import ibis.ipl.PortType;
import ibis.ipl.ReceivePort;
import nl.junglecomputing.pidgin.MessageUpcallChannel;

public class MessageUpcallChannelImpl extends ChannelImpl implements MessageUpcallChannel {

    private static final Logger logger = LoggerFactory.getLogger(ChannelImpl.class);

    private ReceivePort rports[];

    public MessageUpcallChannelImpl(Ibis ibis, String name, MessageUpcall upcall, IbisIdentifier[] ids) throws IOException {
        super(ibis, name);

        System.err.println("Creating ClosedUpcallChannel " + name);

        rports = new ReceivePort[ids.length];

        System.err.println("ClosedUpcallChannel " + name + " has " + ids.length + " members");

        for (int i = 0; i < rports.length; i++) {
            if (!ids[i].equals(ibis.identifier())) {
                rports[i] = ibis.createReceivePort(getPortType(), getReceivePortName(ids[i]), upcall);
                rports[i].enableConnections();

                System.err.println("ClosedChannel created RP " + getReceivePortName(ids[i]));
            }
        }
    }

    @Override
    public void activate() throws IOException {

        boolean wasActive = setActive(true);

        if (wasActive) {
            return;
        }

        for (int i = 0; i < rports.length; i++) {
            if (rports[i] != null) {
                rports[i].enableMessageUpcalls();
            }
        }
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
                rp.disableMessageUpcalls();
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
        return PidginImpl.portTypeOneToOneUpcall;
    }
}
