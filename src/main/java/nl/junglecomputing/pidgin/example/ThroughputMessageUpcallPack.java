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
import ibis.ipl.IbisIdentifier;
import ibis.ipl.MessageUpcall;
import ibis.ipl.ReadMessage;
import ibis.ipl.WriteMessage;
import nl.junglecomputing.pidgin.BufferAllocator;
import nl.junglecomputing.pidgin.CommunicatorException;
import nl.junglecomputing.pidgin.Message;
import nl.junglecomputing.pidgin.MessagePacker;
import nl.junglecomputing.pidgin.OneToOneCommunicatorMessageUpcall;
import nl.junglecomputing.pidgin.Pidgin;

public class ThroughputMessageUpcallPack implements BufferAllocator, MessageUpcall {

    private static final String NAME = "tpU";

    private static final int TESTS = 100;
    private static final int REPEAT = 1000;
    private static final int SIZE = 1024 * 1024;

    private static final byte OPCODE_MSG = 0;
    private static final byte OPCODE_ACK = 1;

    private final ByteBuffer buffer;
    private final ByteBuffer[] buffers;

    private boolean ack = false;
    private int count = 0;

    private final boolean finish;

    private final OneToOneCommunicatorMessageUpcall comm;

    private final Message message;

    public ThroughputMessageUpcallPack(Ibis ibis, boolean finish) throws IOException, CommunicatorException {
        this.finish = finish;

        this.buffer = ByteBuffer.allocate(SIZE);
        this.buffers = new ByteBuffer[1];
        this.buffers[0] = buffer;

        message = new Message(OPCODE_MSG);

        comm = new OneToOneCommunicatorMessageUpcall(NAME, ibis, this);
    }

    @Override
    public void upcall(ReadMessage rm) throws IOException, ClassNotFoundException {

        if (comm.rank() == 0) {

            MessagePacker.unpack(rm, this, message);

            if (finish) {
                rm.finish();
            }

            incCount();

        } else {

            MessagePacker.unpack(rm, this, message);

            if (finish) {
                rm.finish();
            }

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

    public void runTest() throws IOException {

        comm.activate();

        if (comm.rank() == 0) {

            for (int t = 0; t < TESTS; t++) {
                waitForCount();
                WriteMessage wm = comm.send();
                MessagePacker.pack(wm, OPCODE_ACK, null);
                wm.finish();
            }

        } else {
            for (int t = 0; t < TESTS; t++) {

                long start = System.currentTimeMillis();

                for (int r = 0; r < REPEAT; r++) {
                    buffer.position(0);
                    buffer.limit(buffer.capacity());

                    WriteMessage wm = comm.send();
                    MessagePacker.pack(wm, OPCODE_MSG, null, buffers);
                    wm.finish();
                }

                waitForAck();

                long end = System.currentTimeMillis();

                long bytes = (long) SIZE * (long) REPEAT;
                double mbit = ((8 * bytes) / (1000.0 * 1000.0)) / (end - start);

                System.out.println("Test " + t + " took " + (end - start) + " ms. " + mbit + " Gbit/s");
            }
        }

        comm.deactivate();
    }

    @Override
    public ByteBuffer[] allocateByteBuffers(IbisIdentifier sender, byte opcode, Object data, int[] sizes) {
        // only called in the master
        buffer.position(0);
        buffer.limit(buffer.capacity());
        return buffers;
    }

    public static void main(String[] args) throws Exception {

        Properties p = new Properties();

        Ibis ibis = Pidgin.createClosedWorldIbis(p);

        ibis.registry().waitUntilPoolClosed();

        if (ibis.registry().getPoolSize() != 2) {
            System.err.println("Need 2 nodes for this test!");
            System.exit(1);
        }

        new ThroughputMessageUpcallPack(ibis, false).runTest();

        ibis.registry().terminate();
        ibis.registry().waitUntilTerminated();
    }
}