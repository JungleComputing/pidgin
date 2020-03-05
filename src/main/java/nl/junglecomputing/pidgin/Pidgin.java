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

public interface Pidgin {

    public NodeIdentifier getMyIdentifier();

    public NodeIdentifier getMaster();

    public boolean isMaster();

    public NodeIdentifier getElectionResult(String electTag, long timeout) throws IOException;

    public NodeIdentifier elect(String electTag) throws IOException;

    public int getRank();

    public int getPoolSize();

    public NodeIdentifier[] getNodeIdentifiers();

    // Channel management
    public Channel createChannel(String name, Upcall upcall) throws DuplicateChannelException, IOException;

    public Channel getChannel(String name) throws NoSuchChannelException;

    public void removeChannel(String name) throws NoSuchChannelException, IOException;

    // public void activateChannel(String name) throws NoSuchChannelException, IOException;

    // public boolean sendMessage(String channel, NodeIdentifier dest, byte opcode, Object data, ByteBuffer... buffers)
    // throws NoSuchChannelException, ChannelNotActiveException, IOException;

    // public void cleanup();

    // public void cleanup(NodeIdentifier node);

    public void terminate() throws IOException;
}
