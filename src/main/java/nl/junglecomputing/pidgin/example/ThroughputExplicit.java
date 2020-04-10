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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

import ibis.ipl.Ibis;
import ibis.ipl.ReadMessage;
import ibis.ipl.WriteMessage;
import nl.junglecomputing.pidgin.OneToOneCommunicatorExplicit;
import nl.junglecomputing.pidgin.Pidgin;

public class ThroughputExplicit {

    private static final String NAME = "tpEx";

    private static final int TESTS = 100;
    private static final int REPEAT = 1000;
    private static final int SIZE = 1024 * 1024;

    private static final byte OPCODE_ACK = 1;

    private final Ibis ibis;
    private final OneToOneCommunicatorExplicit com;

    public ThroughputExplicit(Ibis ibis) throws Exception {
        this.ibis = ibis;
        com = new OneToOneCommunicatorExplicit(NAME, ibis);
    }

    public void runTest() throws IOException {

        com.activate();

        ByteBuffer buffer = ByteBuffer.allocate(SIZE);

        int rank = com.rank();

        if (rank == 0) {

            for (int t = 0; t < TESTS; t++) {
                for (int r = 0; r < REPEAT; r++) {
                    buffer.position(0);
                    buffer.limit(buffer.capacity());

                    ReadMessage rm = com.receive();
                    rm.readByteBuffer(buffer);
                    rm.finish();
                }

                WriteMessage wm = com.send();
                wm.writeByte(OPCODE_ACK);
                wm.finish();
            }
        } else {
            for (int t = 0; t < TESTS; t++) {

                long start = System.currentTimeMillis();

                for (int r = 0; r < REPEAT; r++) {
                    buffer.position(0);
                    buffer.limit(buffer.capacity());

                    WriteMessage wm = com.send();
                    wm.writeByteBuffer(buffer);
                    wm.finish();
                }

                ReadMessage rm = com.receive();
                rm.readByte();
                rm.finish();

                long end = System.currentTimeMillis();

                long bytes = (long) SIZE * (long) REPEAT;
                double mbit = ((8 * bytes) / (1000.0 * 1000.0)) / (end - start);

                System.out.println("Test " + t + " took " + (end - start) + " ms. " + mbit + " Gbit/s");
            }
        }

        com.deactivate();
    }

    public static void main(String[] args) throws Exception {

        Properties p = new Properties();

        Ibis ibis = Pidgin.createClosedWorldIbis(p);

        ibis.registry().waitUntilPoolClosed();

        if (ibis.registry().getPoolSize() != 2) {
            System.err.println("Need 2 nodes for this test!");
            System.exit(1);
        }

        new ThroughputExplicit(ibis).runTest();

        ibis.registry().terminate();
        ibis.registry().waitUntilTerminated();
    }
}
