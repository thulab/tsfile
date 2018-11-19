package cn.edu.tsinghua.tsfile.common.utils;

import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * this class is to transfer bytebuffer to an inputstream.
 * this class can be removed if all other codes use java NIO.
 */
public class ByteBufferBasedInputStream extends InputStream {

    private ByteBuffer buf;

    public ByteBufferBasedInputStream(ByteBuffer buf) {
        this.buf = buf;
    }

    public int read() {
        if (!buf.hasRemaining()) {
            return -1;
        }
        return buf.get() & 0xFF;
    }

    public int read(byte[] bytes, int off, int len) {
        if (!buf.hasRemaining()) {
            return -1;
        }

        len = Math.min(len, buf.remaining());
        buf.get(bytes, off, len);
        return len;
    }

    @Override
    public int available() {
        return buf.remaining();
    }

}
