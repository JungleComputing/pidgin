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
import ibis.ipl.PortType;
import ibis.ipl.ReceivePort;
import nl.junglecomputing.pidgin.Upcall;
import nl.junglecomputing.timer.Timer;

public class OpenPidginChannel extends PidginChannel {

    private static final Logger logger = LoggerFactory.getLogger(PidginChannel.class);

    private ReceivePort rp;

    public OpenPidginChannel(Ibis ibis, String name, Upcall upcall, Timer timing) throws IOException {
        super(ibis, name, upcall, timing);
    }

    @Override
    public void activate() throws IOException {
        rp = ibis.createReceivePort(getPortType(), getReceivePortName(null), this);
        rp.enableConnections();
    }

    @Override
    protected String getReceivePortName(IbisIdentifier id) {
        return name;
    }

    @Override
    protected void disableReceivePorts() {
        logger.info("disabling receive port");
        rp.disableConnections();
        rp.disableMessageUpcalls();
    }

    @Override
    protected void closeReceivePorts() {
        logger.info("Closing receive port");

        try {
            rp.close(10000);
        } catch (IOException e) {
            logger.info("Close receive port got exception", e);
        }
    }

    @Override
    protected PortType getPortType() {
        return PidginImpl.portTypeManyToOne;
    }

}
