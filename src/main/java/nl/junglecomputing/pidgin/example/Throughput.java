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

import nl.junglecomputing.pidgin.Channel;
import nl.junglecomputing.pidgin.ChannelNotActiveException;
import nl.junglecomputing.pidgin.DuplicateChannelException;
import nl.junglecomputing.pidgin.NoSuchChannelException;
import nl.junglecomputing.pidgin.NodeIdentifier;
import nl.junglecomputing.pidgin.Pidgin;
import nl.junglecomputing.pidgin.PidginFactory;
import nl.junglecomputing.pidgin.Upcall;

public class Throughput implements Upcall {

    private static final String CHANNEL = "tp";

    private static final int TESTS = 100;
    private static final int REPEAT = 1000;
    private static final int SIZE = 1024 * 1024;

    private static final byte OPCODE_DATA = 0;
    private static final byte OPCODE_ACK = 1;

    private final Pidgin pidgin;
    private final ByteBuffer buffer;
    private final ByteBuffer[] buffers;

    private boolean ack = false;
    private int count = 0;

    private final int rank;
    private final NodeIdentifier[] ids;

    private final Channel channel;

    public Throughput(Pidgin pidgin) throws DuplicateChannelException, IOException {
        this.pidgin = pidgin;
        this.buffer = ByteBuffer.allocate(SIZE);
        this.buffers = new ByteBuffer[1];
        this.buffers[0] = buffer;
        rank = pidgin.getRank();
        ids = pidgin.getNodeIdentifiers();

        channel = pidgin.createChannel(CHANNEL, this);
    }

    private synchronized void waitForAck() {
        while (!ack) {
            try {
                wait();
            } catch (Exception e) {
                // ignore
            }
        }

        // System.err.println("woke on ACK");

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

        // System.err.println("woke on " + count + " " + REPEAT);

        count = 0;
    }

    private synchronized void incCount() {
        count++;

        // System.err.println("got message " + count + " " + REPEAT);

        if (count == REPEAT) {
            notifyAll();
        }
    }

    public void runTest() throws DuplicateChannelException, IOException, NoSuchChannelException, ChannelNotActiveException {

        channel.activate();

        int rank = pidgin.getRank();
        NodeIdentifier[] ids = pidgin.getNodeIdentifiers();

        if (rank == 0) {

            for (int t = 0; t < TESTS; t++) {
                waitForCount();
                channel.sendMessage(ids[1], OPCODE_ACK, null);
            }

        } else {
            for (int t = 0; t < TESTS; t++) {

                long start = System.currentTimeMillis();

                for (int r = 0; r < REPEAT; r++) {
                    buffer.position(0);
                    buffer.limit(buffer.capacity());

                    channel.sendMessage(ids[0], OPCODE_DATA, null, buffer);
                }

                waitForAck();

                long end = System.currentTimeMillis();

                long bytes = (long) SIZE * (long) REPEAT;
                double mbit = ((8 * bytes) / (1000.0 * 1000.0)) / (end - start);

                System.out.println("Test " + t + " took " + (end - start) + " ms. " + mbit + " Gbit/s");
            }
        }

        pidgin.removeChannel(CHANNEL);
        pidgin.terminate();
    }

    @Override
    public ByteBuffer[] allocateByteBuffers(String channel, NodeIdentifier sender, byte opcode, Object data, int[] sizes) {
        // only called in the master
        buffer.position(0);
        buffer.limit(buffer.capacity());
        return buffers;
    }

    @Override
    public void receiveMessage(String channel, NodeIdentifier sender, byte opcode, Object data, ByteBuffer[] buffers) {
        if (rank == 0) {
            incCount();
        } else {
            gotAck();
        }
    }

    public static void main(String[] args) throws Exception {

        Properties prop = new Properties();

        Pidgin p = PidginFactory.create(prop);

        if (p.getPoolSize() != 2) {
            System.err.println("Need 2 nodes for this test!");
            p.terminate();
            System.exit(1);
        }

        new Throughput(p).runTest();
    }
}
