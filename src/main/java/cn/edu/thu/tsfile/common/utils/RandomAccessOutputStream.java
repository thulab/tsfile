package cn.edu.thu.tsfile.common.utils;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

/**
 * RandomAccessOutputStream implements the tsfile file writer interface and extends OutputStream. <br>
 * The main difference between RandomAccessOutputStream and general OutputStream
 * is:RandomAccessOutputStream provide method {@code getPos} for random accessing. It also
 * implements {@code getOutputStream} to return an OutputStream supporting tsfile-format
 *
 * @author kangrong
 */
public class RandomAccessOutputStream extends OutputStream implements TSRandomAccessFileWriter {
    private static final String DEFAULT_FILE_MODE = "rw";
    private RandomAccessFile out;

    public RandomAccessOutputStream(File file) throws IOException {
        this(file, DEFAULT_FILE_MODE);
    }

    public RandomAccessOutputStream(File file, String mode) throws IOException {
        out = new RandomAccessFile(file, mode);
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
    }

    @Override
    public void write(byte b[]) throws IOException {
        out.write(b);
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
        out.write(b, off, len);
    }

    public long getPos() throws IOException {
        return out.length();
    }

    public void seek(long offset) throws IOException {
        out.seek(offset);
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    @Override
    public OutputStream getOutputStream() {
        return this;
    }
}
