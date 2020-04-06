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

import ibis.ipl.IbisIdentifier;
import ibis.ipl.MessageUpcall;
import ibis.ipl.ReadMessage;
import ibis.ipl.WriteMessage;
import nl.junglecomputing.pidgin.ChannelNotActiveException;
import nl.junglecomputing.pidgin.DuplicateChannelException;
import nl.junglecomputing.pidgin.MessageUpcallChannel;
import nl.junglecomputing.pidgin.NoSuchChannelException;
import nl.junglecomputing.pidgin.Pidgin;
import nl.junglecomputing.pidgin.PidginFactory;

public class ThroughputMessageUpcallFinish implements MessageUpcall {

    private static final String CHANNEL = "tpMUF";

    private static final int TESTS = 100;
    private static final int REPEAT = 1000;
    private static final int SIZE = 1024 * 1024;

    private static final byte OPCODE_ACK = 1;

    private final ByteBuffer buffer;
    private final ByteBuffer[] buffers;

    private boolean ack = false;
    private int count = 0;

    private final int rank;
    private final IbisIdentifier[] ids;

    private final MessageUpcallChannel channel;

    public ThroughputMessageUpcallFinish(Pidgin pidgin) throws DuplicateChannelException, IOException {
        this.buffer = ByteBuffer.allocate(SIZE);
        this.buffers = new ByteBuffer[1];
        this.buffers[0] = buffer;

        rank = pidgin.getRank();
        ids = pidgin.getAllIdentifiers();

        channel = pidgin.createMessageUpcallChannel(CHANNEL, this);
    }

    @Override
    public void upcall(ReadMessage readMessage) throws IOException, ClassNotFoundException {
        if (rank == 0) {
            buffer.position(0);
            buffer.limit(buffer.capacity());
            readMessage.readByteBuffer(buffer);
            readMessage.finish();
            incCount();
        } else {
            readMessage.readByte();
            readMessage.finish();
            gotAck();
        }
    }

    private synchronized void waitForAck() {
        while (!ack) {
            try {
                wait();
            } catch (Exception e) {
                // ignore
            }
        }

        ack = false;
    }

    private synchronized void gotAck() {
        ack = true;
        notifyAll();
    }

    private synchronized void waitForCount() {
        while (count < REPEAT) {
            try {
                wait();
            } catch (Exception e) {
                // ignore
            }
        }

        count = 0;
    }

    private synchronized void incCount() {
        count++;

        if (count == REPEAT) {
            notifyAll();
        }
    }

    public void runTest() throws DuplicateChannelException, IOException, NoSuchChannelException, ChannelNotActiveException {

        channel.activate();

        if (rank == 0) {

            for (int t = 0; t < TESTS; t++) {
                waitForCount();
                WriteMessage wm = channel.sendMessage(ids[1]);
                wm.writeByte(OPCODE_ACK);
                wm.finish();
            }

        } else {
            for (int t = 0; t < TESTS; t++) {

                long start = System.currentTimeMillis();

                for (int r = 0; r < REPEAT; r++) {
                    buffer.position(0);
                    buffer.limit(buffer.capacity());

                    WriteMessage wm = channel.sendMessage(ids[0]);
                    wm.writeByteBuffer(buffer);
                    wm.finish();
                }

                waitForAck();

                long end = System.currentTimeMillis();

                long bytes = (long) SIZE * (long) REPEAT;
                double mbit = ((8 * bytes) / (1000.0 * 1000.0)) / (end - start);

                System.out.println("Test " + t + " took " + (end - start) + " ms. " + mbit + " Gbit/s");
            }
        }

        channel.deactivate();
    }

    public static void main(String[] args) throws Exception {

        Properties prop = new Properties();

        Pidgin p = PidginFactory.create("TP", prop);

        if (p.getPoolSize() != 2) {
            System.err.println("Need 2 nodes for this test!");
            System.exit(1);
        }

        new ThroughputMessageUpcallFinish(p).runTest();

        PidginFactory.terminate("TP");
    }

}
