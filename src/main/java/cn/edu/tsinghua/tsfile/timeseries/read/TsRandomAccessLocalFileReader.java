package cn.edu.tsinghua.tsfile.timeseries.read;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;

/**
 * File-read interface for local file.
 *
 * @author Jinrui Zhang
 */
public class TsRandomAccessLocalFileReader implements ITsRandomAccessFileReader {

  /** the wrapped RandomAccessFile by this class **/
  private RandomAccessFile raf;

  /**
   * init RandomAccessFile with read mode
   * @param filePath the file path for this TsFile
   * @throws FileNotFoundException
   */
  public TsRandomAccessLocalFileReader(String filePath) throws FileNotFoundException {
    this.raf = new RandomAccessFile(filePath, "r");
  }

  /**
   * init RandomAccessFile by the input RandomAccessFile
   * @param raf input RandomAccessFile
   */
  public TsRandomAccessLocalFileReader(RandomAccessFile raf) {
    this.raf = raf;
  }

  /**
   * put the pointer to offset location
   * @param offset the location of the target
   * @throws IOException
   */
  @Override
  public void seek(long offset) throws IOException {
    this.raf.seek(offset);
  }

  /**
   * Reads a byte of data from this file. The byte is returned as aninteger in the range 0 to 255 ({@code 0x00-0x0ff}).
   * This method blocks if no input is yet available.
   * @return the next byte of data, or {@code -1} if the end of the file has been reached.
   * @throws IOException
   */
  @Override
  public int read() throws IOException {
    return raf.read();
  }

  /**
   * read some bytes from file. This method will try to make sure {@code len} bytes are all read.
   * @param b the bytes array to store read bytes
   * @param off the start position of the read data
   * @param len the length of the read data
   * @return the actual len of data read by this method
   * @throws IOException
   */
  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int end = len + off;
    int get = 1;
    int total = 0;
    // keep reading until {@code len} bytes are read
    for (int i = off; i < end; i += get) {
      // try to read all data
      get = raf.read(b, i, end - i);
      // record the length of bytes read in this loop
      if (get > 0)
        total += get;
      else
        // can't read more data
        break;
    }
    return total;
  }

  /**
   * Returns the length of this file.
   *
   * @return     the length of this file, measured in bytes.
   * @exception  IOException  if an I/O error occurs.
   */
  @Override
  public long length() throws IOException {
    return raf.length();
  }

  /**
   * Reads a signed 32-bit integer from this file. This method reads 4 bytes from the file,
   * starting at the current file pointer.
   * This method blocks until the four bytes are read, the end of the stream is detected, or an exception is thrown.
   *
   * @return the next four bytes of this file, interpreted as an {@code int}.
   * @exception IOException if an I/O error occurs.
   */
  @Override
  public int readInt() throws IOException {
    return raf.readInt();
  }

  /**
   * Closes this random access file stream and releases any system resources associated with the stream.
   * A closed random access file cannot perform input or output operations and cannot be reopened.
   *
   * <p> If this file has an associated channel then the channel is closed
   * as well.
   *
   * @exception IOException if an I/O error occurs.
   */
  @Override
  public void close() throws IOException {
    raf.close();
  }
}
