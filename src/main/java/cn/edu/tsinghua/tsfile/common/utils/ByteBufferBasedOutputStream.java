package cn.edu.tsinghua.tsfile.common.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * this class is to transfer bytebuffer to an inputstream.
 * this class can be removed if all other codes use java NIO.
 */
public class ByteBufferBasedOutputStream extends OutputStream {
    ByteBuffer buf;

    public ByteBufferBasedOutputStream(ByteBuffer buf) {
        this.buf = buf;
    }

    public void write(int b) throws IOException {
        buf.put((byte) b);
    }

    public void write(byte[] bytes, int off, int len)
            throws IOException {
        buf.put(bytes, off, len);
    }

}
