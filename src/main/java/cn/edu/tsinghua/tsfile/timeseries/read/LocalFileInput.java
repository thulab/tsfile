package cn.edu.tsinghua.tsfile.timeseries.read;

import cn.edu.tsinghua.tsfile.common.utils.TSRandomAccessFileReader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * File-read interface for local file.
 *
 * @author Jinrui Zhang
 */
public class LocalFileInput implements TSRandomAccessFileReader {

    private RandomAccessFile raf;

    public LocalFileInput(String path) throws FileNotFoundException {
        this.raf = new RandomAccessFile(path, "r");
    }

    @Override
    public void seek(long offset) throws IOException {
        this.raf.seek(offset);
    }

    @Override
    public int read() throws IOException {
        return raf.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return raf.read(b, off, len);
    }

    @Override
    public long length() throws IOException {
        return raf.length();
    }

    @Override
    public int readInt() throws IOException {
        return raf.readInt();
    }

    @Override
    public void close() throws IOException {
        raf.close();
    }
}
