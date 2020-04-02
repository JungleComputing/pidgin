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
import ibis.ipl.MessageUpcall;
import ibis.ipl.PortType;
import nl.junglecomputing.pidgin.MessageUpcallChannel;

public class MessageUpcallChannelImpl extends ChannelImpl implements MessageUpcallChannel {

    public MessageUpcallChannelImpl(Ibis ibis, String name, MessageUpcall upcall, IbisIdentifier[] ids) throws IOException {
        super(ibis, name, ids, upcall);
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
        return name + "_MUC_" + id.name();
    }

    @Override
    protected PortType getPortType() {
        return PidginImpl.portTypeOneToOneUpcall;
    }
}
