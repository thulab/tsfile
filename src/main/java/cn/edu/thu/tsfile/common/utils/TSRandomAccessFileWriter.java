package cn.edu.thu.tsfile.common.utils;

import java.io.IOException;
import java.io.OutputStream;

/**
 * TSRandomAccessFileWriter is an interface for TSFile writer. Each output should implements this
 * interface whatever os file system or HDFS.<br>
 * The main difference between RandomAccessOutputStream and general OutputStream
 * is:RandomAccessOutputStream provide method {@code getPos} for random accessing. It also
 * implements {@code getOutputStream} to return an OutputStream supporting tsfile-format
 * 
 * @author kangrong
 *
 */
public interface TSRandomAccessFileWriter {
  public long getPos() throws IOException;

  public void write(byte[] b) throws IOException;

  public void write(int b) throws IOException;

  public void close() throws IOException;

  public OutputStream getOutputStream();
}
