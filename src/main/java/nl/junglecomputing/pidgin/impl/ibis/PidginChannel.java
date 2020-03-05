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
package nl.junglecomputing.pidgin.impl.ibis;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.MessageUpcall;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;
import nl.junglecomputing.pidgin.Channel;
import nl.junglecomputing.pidgin.NodeIdentifier;
import nl.junglecomputing.pidgin.Upcall;

public abstract class PidginChannel implements Channel, MessageUpcall {

    private static final Logger logger = LoggerFactory.getLogger(PidginChannel.class);

    private static final long CONNECT_TIMEOUT = 60 * 1000;

    protected final Ibis ibis;

    protected final String name;

    private Upcall upcall;

    private final ConcurrentHashMap<IbisIdentifier, SendPort> sendports = new ConcurrentHashMap<IbisIdentifier, SendPort>();

    protected PidginChannel(Ibis ibis, String name, Upcall upcall) {
        this.ibis = ibis;
        this.name = name;
        this.upcall = upcall;
    }

    protected final String getName() {
        return name;
    }

    public abstract void activate() throws IOException;

    private void closeSendPorts() {

        logger.debug("Closing sendports");

        for (SendPort sp : sendports.values()) {
            try {
                sp.close();
            } catch (Exception e) {
                logger.info("Close sendport got exception", e);
            }
        }

        logger.debug("All sendports closed");
    }

    @Override
    public void deactivate() throws IOException {

        disableReceivePorts();

        closeSendPorts();

        closeReceivePorts();
    }

    @Override
    public void disconnect(NodeIdentifier dest) {

        IbisIdentifier id = ((NodeIdentifierImpl) dest).getIbisIdentifier();

        SendPort s = sendports.remove(id);

        if (s != null) {
            try {
                s.close();
            } catch (Exception e) {
                logger.warn("Failed to close sendport", e);
            }
        }
    }

    protected abstract String getReceivePortName(IbisIdentifier id);

    protected abstract void disableReceivePorts();

    protected abstract void closeReceivePorts();

    protected abstract PortType getPortType();

    private SendPort createAndConnect(IbisIdentifier id, String rpName, long timeout) throws IOException {

        // if (logger.isInfoEnabled()) {
        System.out.println("Connecting to " + id.name() + ":" + rpName + " from " + ibis.identifier());
        // }

        SendPort sp = null;

        try {
            sp = ibis.createSendPort(getPortType());
            sp.connect(id, rpName, timeout, true);

            System.out.println("Connection to " + id.name() + ":" + rpName + " established");

        } catch (IOException e) {
            try {
                sp.close();
            } catch (Throwable e2) {
                // ignored ?
            }
            logger.error("Could not connect to " + id.name() + ":" + rpName, e);
            throw e;
        }
        return sp;
    }

    private SendPort getSendPort(IbisIdentifier id) throws IOException {

        if (id.equals(ibis.identifier())) {
            logger.error("Sending to myself! " + id + " " + ibis.identifier(), new Throwable());
        }

        SendPort sp = sendports.get(id);

        if (sp == null) {
            String rpName = getReceivePortName(ibis.identifier());

            sp = createAndConnect(id, rpName, CONNECT_TIMEOUT);

            if (logger.isInfoEnabled()) {
                logger.info("Succesfully connected to " + id + ":" + rpName + " from " + ibis.identifier());
            }

            SendPort sp2 = sendports.putIfAbsent(id, sp);

            if (sp2 != null) {
                // Someone managed to sneak in between our get and put!
                try {
                    sp.close();
                } catch (Exception e) {
                    // ignored
                }

                sp = sp2;
            }
        }

        return sp;
    }

    private boolean doSendMessage(SendPort s, IbisIdentifier dest, byte opcode, Object data, ByteBuffer... buffers) {

        // System.out.println("doSendMessage to " + dest);

        int eventNo = -1;
        long sz = 0;
        WriteMessage wm = null;
        try {
            wm = s.newMessage();
            // String name = Pool.getString(m.opcode, "write");
            // if (communicationTimer != null && m.contents != null) {
            // eventNo = communicationTimer.start(name);
            // }

            wm.writeByte(opcode);

            if (data == null) {
                wm.writeBoolean(false);
            } else {
                wm.writeBoolean(true);
                wm.writeObject(data);
            }

            if (buffers == null || buffers.length == 0) {
                wm.writeInt(0);
            } else {
                wm.writeInt(buffers.length);

                for (ByteBuffer b : buffers) {
                    if (b == null) {
                        wm.writeInt(0);
                    } else {
                        wm.writeInt(b.remaining());
                    }
                }

                for (ByteBuffer b : buffers) {
                    if (b != null) {
                        wm.writeByteBuffer(b);
                    }
                }
            }

            sz = wm.finish();
            // if (eventNo != -1) {
            // communicationTimer.stop(eventNo);
            // communicationTimer.addBytes(sz, eventNo);
            // }
        } catch (IOException e) {
            logger.warn("Communication to " + dest + " gave exception", e);
            if (wm != null) {
                wm.finish(e);
            }
            // if (eventNo != -1) {
            // communicationTimer.cancel(eventNo);
            // }
            return false;
        }

        // System.out.println("doSendMessage to " + dest + " success");

        return true;
    }

    public boolean sendMessage(NodeIdentifier destination, byte opcode, Object data, ByteBuffer... buffers) {

        SendPort s = null;
        IbisIdentifier dest = ((NodeIdentifierImpl) destination).getIbisIdentifier();

        // System.out.println("SendMessage to " + dest);

        try {
            s = getSendPort(dest);
        } catch (IOException e) {
            logger.warn("Failed to connect to " + dest, e);
            return false;
        }

        // System.out.println("SendMessage to " + dest + " got sendport");

        return doSendMessage(s, dest, opcode, data, buffers);
    }

    @Override
    public void upcall(ReadMessage rm) throws IOException, ClassNotFoundException {

        NodeIdentifier source = new NodeIdentifierImpl(rm.origin().ibisIdentifier());
        int timerEvent = -1;
        byte opcode = rm.readByte();

        boolean hasObject = rm.readBoolean();

        Object data = null;

        if (hasObject) {
            data = rm.readObject();
        }

        int bufferCount = rm.readInt();

        ByteBuffer[] buffers = null;

        if (bufferCount > 0) {
            int[] sizes = new int[bufferCount];

            for (int i = 0; i < bufferCount; i++) {
                sizes[i] = rm.readInt();
            }

            buffers = upcall.allocateByteBuffers(name, source, opcode, data, sizes);

            for (int i = 0; i < bufferCount; i++) {
                // TODO: We should check if the buffers[i] is actually valid and has the reading space?
                rm.readByteBuffer(buffers[i]);
            }
        }

        long sz = rm.finish();

        upcall.receiveMessage(name, source, opcode, data, buffers);

        // if (communicationTimer != null && hasObject) {
        // timerEvent = communicationTimer.start(Pool.getString(opcode, "read"));
        // }

        // if (timerEvent != -1) {
        // if (data == null) {
        // communicationTimer.cancel(timerEvent);
        // } else {
        // communicationTimer.stop(timerEvent);
        // communicationTimer.addBytes(sz, timerEvent);
        // }
        // }
    }
}
