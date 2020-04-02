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

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.MessageUpcall;
import ibis.ipl.ReadMessage;
import ibis.ipl.WriteMessage;
import nl.junglecomputing.pidgin.Upcall;
import nl.junglecomputing.pidgin.UpcallChannel;

public class UpcallChannelImpl implements UpcallChannel, MessageUpcall {

    private final MessageUpcallChannelImpl impl;
    private final Upcall upcall;

    public UpcallChannelImpl(Ibis ibis, String name, Upcall upcall, IbisIdentifier[] ids) throws IOException {
        this.upcall = upcall;
        impl = new MessageUpcallChannelImpl(ibis, name, this, ids);
    }

    @Override
    public void activate() throws IOException {
        impl.activate();
    }

    @Override
    public void deactivate() throws IOException {
        impl.deactivate();
    }

    @Override
    public void sendMessage(IbisIdentifier dest, byte opcode, Object data, ByteBuffer... buffers) throws IOException {

        WriteMessage wm = impl.sendMessage(dest);

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

        wm.finish();
    }

    @Override
    public void upcall(ReadMessage rm) throws IOException, ClassNotFoundException {

        IbisIdentifier source = rm.origin().ibisIdentifier();

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

            buffers = upcall.allocateByteBuffers(impl.getName(), source, opcode, data, sizes);

            for (int i = 0; i < bufferCount; i++) {
                // TODO: We should check if the buffers[i] is actually valid and has the reading space?
                rm.readByteBuffer(buffers[i]);
            }
        }

        rm.finish();

        upcall.receiveMessage(impl.getName(), source, opcode, data, buffers);
    }
}
