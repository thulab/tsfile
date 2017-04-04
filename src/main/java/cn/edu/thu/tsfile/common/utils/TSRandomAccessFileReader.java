package cn.edu.thu.tsfile.common.utils;

import java.io.IOException;

/**
 * Interface for file read.
 * @author Jinrui Zhang
 *
 */
public interface TSRandomAccessFileReader {

    public void seek(long offset) throws IOException;

    public int read() throws IOException;

    public int read(byte[] b, int off, int len) throws IOException;

    public long length() throws IOException;

    public int readInt() throws IOException;
    
    public void close() throws IOException;
}
