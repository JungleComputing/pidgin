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
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;
import nl.junglecomputing.pidgin.Channel;
import nl.junglecomputing.pidgin.ChannelNotActiveException;

public abstract class PidginChannel implements Channel {

    private static final Logger logger = LoggerFactory.getLogger(PidginChannel.class);

    private static final long CONNECT_TIMEOUT = 60 * 1000;

    protected final Ibis ibis;

    protected final String name;

    private boolean active = false;

    private final ConcurrentHashMap<IbisIdentifier, SendPort> sendports = new ConcurrentHashMap<IbisIdentifier, SendPort>();

    protected PidginChannel(Ibis ibis, String name) {
        this.ibis = ibis;
        this.name = name;
    }

    protected final String getName() {
        return name;
    }

    protected final synchronized boolean setActive(boolean value) {
        boolean old = active;
        active = value;
        return old;
    }

    protected final synchronized boolean isActive() {
        return active;
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

        boolean wasActive = setActive(false);

        if (!wasActive) {
            return;
        }

        disableReceivePorts();

        closeSendPorts();

        closeReceivePorts();
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

    public WriteMessage sendMessage(IbisIdentifier destination) throws IOException {

        if (!isActive()) {
            throw new ChannelNotActiveException("Cannot send message, channel " + name + " not active");
        }

        try {
            return getSendPort(destination).newMessage();
        } catch (IOException e) {
            logger.warn("Failed to connect to " + destination, e);
            throw e;
        }
    }

    /*
     * public boolean sendMessage(NodeIdentifier destination, byte opcode, Object data, ByteBuffer... buffers) {
     * 
     * if (!isActive()) { return false; }
     * 
     * long sz = 0; int eventNo = -1; SendPort s = null; WriteMessage wm = null;
     * 
     * if (communicationTimer != null) { eventNo = communicationTimer.start("pidgin " + name + " send message"); }
     * 
     * IbisIdentifier dest = ((NodeIdentifierImpl) destination).getIbisIdentifier();
     * 
     * try { s = getSendPort(dest); } catch (IOException e) { logger.warn("Failed to connect to " + dest, e); return false; }
     * 
     * try { wm = s.newMessage(); wm.writeByte(opcode);
     * 
     * if (data == null) { wm.writeBoolean(false); } else { wm.writeBoolean(true); wm.writeObject(data); }
     * 
     * if (buffers == null || buffers.length == 0) { wm.writeInt(0); } else { wm.writeInt(buffers.length);
     * 
     * for (ByteBuffer b : buffers) { if (b == null) { wm.writeInt(0); } else { wm.writeInt(b.remaining()); } }
     * 
     * for (ByteBuffer b : buffers) { if (b != null) { wm.writeByteBuffer(b); } } }
     * 
     * sz = wm.finish();
     * 
     * if (eventNo != -1) { communicationTimer.stop(eventNo); communicationTimer.addBytes(sz, eventNo); } } catch (IOException e) {
     * logger.warn("Communication to " + dest + " gave exception", e); if (wm != null) { wm.finish(e); } if (eventNo != -1) {
     * communicationTimer.cancel(eventNo); } return false; }
     * 
     * return true; }
     */
    // @Override
    // public void upcall(ReadMessage rm) throws IOException, ClassNotFoundException {
    //
    // NodeIdentifier source = new NodeIdentifierImpl(rm.origin().ibisIdentifier());
    // int timerEvent = -1;
    //
    // if (communicationTimer != null) {
    // timerEvent = communicationTimer.start("pidgin read message");
    // }
    //
    // byte opcode = rm.readByte();
    // boolean hasObject = rm.readBoolean();
    //
    // Object data = null;
    //
    // if (hasObject) {
    // data = rm.readObject();
    // }
    //
    // int bufferCount = rm.readInt();
    //
    // ByteBuffer[] buffers = null;
    //
    // if (bufferCount > 0) {
    // int[] sizes = new int[bufferCount];
    //
    // for (int i = 0; i < bufferCount; i++) {
    // sizes[i] = rm.readInt();
    // }
    //
    // buffers = upcall.allocateByteBuffers(name, source, opcode, data, sizes);
    //
    // for (int i = 0; i < bufferCount; i++) {
    // // TODO: We should check if the buffers[i] is actually valid and has the reading space?
    // rm.readByteBuffer(buffers[i]);
    // }
    // }
    //
    // long sz = rm.finish();
    //
    // if (timerEvent != -1) {
    // communicationTimer.stop(timerEvent);
    // communicationTimer.addBytes(sz, timerEvent);
    // }
    //
    // upcall.receiveMessage(name, source, opcode, data, buffers);
    // }
}
