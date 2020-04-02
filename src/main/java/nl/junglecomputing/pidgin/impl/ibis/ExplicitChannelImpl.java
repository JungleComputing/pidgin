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

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import nl.junglecomputing.pidgin.ExplicitChannel;
import nl.junglecomputing.pidgin.NoSuchSourceException;

public class ExplicitChannelImpl extends ChannelImpl implements ExplicitChannel {

    public ExplicitChannelImpl(Ibis ibis, String name, IbisIdentifier[] ids) throws IOException {
        super(ibis, name, ids);
    }

    @Override
    public void activate() throws IOException {
        setActive(true);
    }

    @Override
    protected String getReceivePortName(IbisIdentifier id) {
        return name + "_EX_" + id.name();
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
