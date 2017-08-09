package cn.edu.tsinghua.tsfile.timeseries.read;

import cn.edu.tsinghua.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.management.FileStreamManager;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Jinrui Zhang
 * @description File-read interface for local file.
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

    public int read() throws IOException {
        return raf.read();
    }

    public int read(byte[] b, int off, int len) throws IOException {
        return raf.read(b, off, len);
    }

    public long length() throws IOException {
        return raf.length();
    }

    @Override
    public int readInt() throws IOException {
        return raf.readInt();
    }

    /**
     * use {@code FileStreamManager} to manage all LocalFileInput
     */
    public void closeFromManager() {
        FileStreamManager.getInstance().close(this);
    }

    @Override
    public void close() throws IOException {
        raf.close();
    }
}
