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

import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReadMessage;
import ibis.ipl.WriteMessage;

public interface ExplicitChannel extends Channel {

    public WriteMessage sendMessage(IbisIdentifier dest) throws IOException;

    public ReadMessage receiveMessage(IbisIdentifier source, long timeout) throws IOException;

    public default ReadMessage receiveMessage(IbisIdentifier source) throws IOException {
        return receiveMessage(source, 0L);
    }
}
