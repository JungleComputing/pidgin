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
import java.nio.ByteBuffer;

import ibis.ipl.ReadMessage;
import ibis.ipl.WriteMessage;

public class MessagePacker {

    public static void pack(WriteMessage wm, Message m) throws IOException {
        pack(wm, m.getOpcode(), m.getData(), m.getBuffers());
    }

    public static void pack(WriteMessage wm, byte opcode, Object data, ByteBuffer... buffers) throws IOException {

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
    }

    public static Message unpack(ReadMessage rm, BufferAllocator allocator) throws IOException, ClassNotFoundException {
        return unpack(rm, allocator, null);
    }

    public static Message unpack(ReadMessage rm, BufferAllocator allocator, Message m) throws IOException, ClassNotFoundException {

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

            buffers = allocator.allocateByteBuffers(rm.origin().ibisIdentifier(), opcode, data, sizes);

            for (int i = 0; i < bufferCount; i++) {
                // TODO: We should check if the buffers[i] is actually valid and has the reading space?
                rm.readByteBuffer(buffers[i]);
            }
        }

        if (m == null) {
            m = new Message(opcode, data, buffers);
        } else {
            m.setOpcode(opcode);
            m.setData(data);
            m.setBuffers(buffers);
        }

        return m;
    }
}
