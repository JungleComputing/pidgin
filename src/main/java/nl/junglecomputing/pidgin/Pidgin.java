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

import java.util.Properties;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisCreationFailedException;
import ibis.ipl.IbisFactory;
import ibis.ipl.PortType;
import ibis.ipl.RegistryEventHandler;

/**
 * Pidgin provides set of utility functions to create an open or closed world Ibis instance with a default set of features that most application will need:
 * one-to-one, many-to-one and one-to-many communication; object serialization, receiving through upcalls or explicit receive, and strict elections.
 * 
 * @author Jason Maassen
 */

public class Pidgin {

    public static long CONNECT_TIMEOUT = 30 * 1000L;

    public static final PortType portTypeManyToOneUpcall = new PortType(PortType.COMMUNICATION_FIFO, PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_OBJECT, PortType.RECEIVE_AUTO_UPCALLS, PortType.RECEIVE_TIMEOUT, PortType.CONNECTION_MANY_TO_ONE);

    public static final PortType portTypeOneToOneUpcall = new PortType(PortType.COMMUNICATION_FIFO, PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_OBJECT, PortType.RECEIVE_AUTO_UPCALLS, PortType.RECEIVE_TIMEOUT, PortType.CONNECTION_ONE_TO_ONE,
            PortType.CONNECTION_UPCALLS);

    public static final PortType portTypeOneToManyUpcall = new PortType(PortType.COMMUNICATION_FIFO, PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_OBJECT, PortType.RECEIVE_AUTO_UPCALLS, PortType.RECEIVE_TIMEOUT, PortType.CONNECTION_ONE_TO_MANY);

    public static final PortType portTypeOneToOneExplicit = new PortType(PortType.COMMUNICATION_FIFO, PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_OBJECT, PortType.RECEIVE_EXPLICIT, PortType.RECEIVE_TIMEOUT, PortType.RECEIVE_TIMEOUT, PortType.CONNECTION_ONE_TO_ONE,
            PortType.CONNECTION_UPCALLS);

    public static final PortType portTypeManyToOneExplicit = new PortType(PortType.COMMUNICATION_FIFO, PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_OBJECT, PortType.RECEIVE_EXPLICIT, PortType.RECEIVE_TIMEOUT, PortType.RECEIVE_TIMEOUT, PortType.CONNECTION_MANY_TO_ONE);

    public static final PortType portTypeOneToManyExplicit = new PortType(PortType.COMMUNICATION_FIFO, PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_OBJECT, PortType.RECEIVE_EXPLICIT, PortType.RECEIVE_TIMEOUT, PortType.RECEIVE_TIMEOUT, PortType.CONNECTION_ONE_TO_MANY);

    private static final PortType[] portTypes = new PortType[] { portTypeManyToOneUpcall, portTypeOneToOneUpcall, portTypeOneToManyUpcall,
            portTypeOneToOneExplicit, portTypeManyToOneExplicit, portTypeManyToOneExplicit };

    public static final IbisCapabilities closedIbisCapabilities = new IbisCapabilities(IbisCapabilities.CLOSED_WORLD, IbisCapabilities.TERMINATION,
            IbisCapabilities.ELECTIONS_STRICT, IbisCapabilities.MEMBERSHIP_TOTALLY_ORDERED);

    public static final IbisCapabilities openIbisCapabilities = new IbisCapabilities(IbisCapabilities.MALLEABLE, IbisCapabilities.TERMINATION,
            IbisCapabilities.ELECTIONS_STRICT, IbisCapabilities.MEMBERSHIP_TOTALLY_ORDERED);

    /**
     * Create a closed world Ibis with the default set of features most application will need: one-to-one, many-to-one and one-to-many communication; object
     * serialization, receive through upcalls or explicit receive, and strict elections.
     * 
     * @param properties
     *            Additional properties to pass to the Ibis being created
     * @return the Ibis instance that was created
     * @throws IbisCreationFailedException
     *             if the creation failed
     */
    public static Ibis createClosedWorldIbis(Properties properties) throws IbisCreationFailedException {
        return IbisFactory.createIbis(closedIbisCapabilities, properties, true, null, portTypes);
    }

    /**
     * Create a open world Ibis with a default set of features most application will need: one-to-one, many-to-one and one-to-many communication; object
     * serialization, receive through upcalls or explicit receive, and strict elections.
     * 
     * @param properties
     *            Additional properties to pass to the Ibis being created
     * @param handler
     *            The callback that will be invoked when Ibis instances join or leave the pool
     * @return the Ibis instance that was created
     * @throws IbisCreationFailedException
     *             if the creation failed
     */
    public static Ibis createOpenWorldIbis(Properties properties, RegistryEventHandler handler) throws IbisCreationFailedException {
        return IbisFactory.createIbis(openIbisCapabilities, properties, true, handler, portTypes);
    }
}
