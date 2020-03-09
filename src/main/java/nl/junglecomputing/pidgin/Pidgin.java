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

/**
 * Pidgin provided a simple communication layer based on a fixed set of nodes.
 * 
 * @author jason
 */

public interface Pidgin {

    public abstract int getRank();

    public abstract int getPoolSize();

    public abstract NodeIdentifier[] getNodeIdentifiers();

    public abstract NodeIdentifier getMyIdentifier();

    public abstract NodeIdentifier getMaster();

    public abstract boolean isMaster();

    // Channel management
    public abstract Channel createChannel(String name, Upcall upcall) throws DuplicateChannelException, IOException;

    public abstract Channel getChannel(String name) throws NoSuchChannelException;

    public abstract void removeChannel(String name) throws NoSuchChannelException, IOException;

}
