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

import nl.junglecomputing.timer.Timer;

/**
 * Pidgin provided a simple communication layer based on a fixed set of nodes.
 * 
 * @author jason
 */

public interface Pidgin {

    public int getRank();

    public int getPoolSize();

    public NodeIdentifier[] getNodeIdentifiers();

    public NodeIdentifier getMyIdentifier();

    public NodeIdentifier getMaster();

    public boolean isMaster();

    public NodeIdentifier getElectionResult(String electTag, long timeout) throws IOException;

    public NodeIdentifier elect(String electTag) throws IOException;

    // Channel management
    public Channel createChannel(String name, Upcall upcall, Timer profiling) throws DuplicateChannelException, IOException;

    public Channel getChannel(String name) throws NoSuchChannelException;

    public void removeChannel(String name) throws NoSuchChannelException, IOException;

}
