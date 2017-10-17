package cn.edu.tsinghua.tsfile.timeseries.read;

import cn.edu.tsinghua.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.management.FileStreamManager;

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

  public int read() throws IOException {
    return raf.read();
  }

  public int read(byte[] b, int off, int len) throws IOException {
    int end = len + off;
    int get = 1;
    int total = 0;
    for (int i = off; i < end; i += get) {
      get = raf.read(b, i, end - i);
      if (get > 0)
        total += get;
      else
        break;
    }
    return total;
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
