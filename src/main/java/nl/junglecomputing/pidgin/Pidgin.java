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

/**
 * Pidgin provided a simple communication layer based on a fixed set of nodes.
 * 
 * @author jason
 */

public interface Pidgin {

    public Ibis getIbis();

    public int getRank();

    public int getPoolSize();

    public boolean isMaster();

    public IbisIdentifier[] getAllIdentifiers();

    public IbisIdentifier getMyIdentifier();

    public IbisIdentifier getMaster();

    public IbisIdentifier getElectionResult(String electTag, long timeout) throws IOException;

    public IbisIdentifier elect(String electTag) throws IOException;

    // Create communication channels for upcall and explicit receipt.
    public MessageUpcallChannel createUpcallChannel(String name, IbisIdentifier[] participants, MessageUpcall upcall) throws DuplicateChannelException, IOException;

    public default MessageUpcallChannel createUpcallChannel(String name, MessageUpcall upcall) throws DuplicateChannelException, IOException {
        return createUpcallChannel(name, getAllIdentifiers(), upcall);
    }

    public ExplicitChannel createExplicitChannel(String name, IbisIdentifier[] participants) throws DuplicateChannelException, IOException;

    public default ExplicitChannel createExplicitChannel(String name) throws DuplicateChannelException, IOException {
        return createExplicitChannel(name, getAllIdentifiers());
    }
}
