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
package nl.junglecomputing.pidgin.example;

import java.util.Properties;

import ibis.ipl.Ibis;
import nl.junglecomputing.pidgin.Pidgin;

public class ThroughputMessageUpcallPackFinish {

    public static void main(String[] args) throws Exception {

        Properties p = new Properties();

        Ibis ibis = Pidgin.createClosedWorldIbis(p);

        ibis.registry().waitUntilPoolClosed();

        if (ibis.registry().getPoolSize() != 2) {
            System.err.println("Need 2 nodes for this test!");
            System.exit(1);
        }

        new ThroughputMessageUpcallPack(ibis, true).runTest();

        ibis.registry().terminate();
        ibis.registry().waitUntilTerminated();
    }

}
