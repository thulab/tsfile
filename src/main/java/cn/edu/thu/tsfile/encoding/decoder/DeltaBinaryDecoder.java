package cn.edu.thu.tsfile.encoding.decoder;

import java.io.IOException;
import java.io.InputStream;

import cn.edu.thu.tsfile.encoding.encoder.DeltaBinaryEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.utils.BytesUtils;
import cn.edu.thu.tsfile.file.metadata.enums.TSEncoding;

/**
 * This class is a decoder for decoding the byte array that encoded by
 * {@code DeltaBinaryEncoder}.DeltaBinaryDecoder just supports integer and long values.<br>
 * 
 * @see DeltaBinaryEncoder
 * @author kangrong
 *
 * @param <T>
 */
public abstract class DeltaBinaryDecoder extends Decoder {
  private static final Logger LOG = LoggerFactory.getLogger(DeltaBinaryDecoder.class);
  protected long count = 0;
  protected byte[] deltaBuf;
  
  /**
   * the first value in one pack.
   */
  protected int readIntTotalCount = 0;
  protected int nextReadIndex = 0;
  /**
   * max bit length of all value in a pack
   */
  protected int packWidth;
  /**
   * data number in this pack
   */
  protected int packNum;
  
  /**
   * how many bytes data takes after encoding
   */
  protected int encodingLength;

  public DeltaBinaryDecoder() {
    super(TSEncoding.TS_2DIFF);
  }

  protected abstract void readHeader(InputStream in) throws IOException;

  protected abstract void allocateDataArray();

  protected abstract void readValue(int i);

  /**
   * calculate the bytes length containing v bits
   * 
   * @param v - number of bits
   * @return number of bytes
   */
  protected int ceil(int v) {
    return (int) Math.ceil((double) (v) / 8.0);
  }

  

  @Override
  public boolean hasNext(InputStream in) throws IOException {
    return (nextReadIndex < readIntTotalCount) || in.available() > 0;
  }

  

  public static class IntDeltaDecoder extends DeltaBinaryDecoder {
	  private int firstValue;
	  private int[] data;
	  private int previous;
	  /**
	   * minimum value for all difference
	   */
	  private int minDeltaBase;
	  
    public IntDeltaDecoder() {
      super();
    }

    /**
     * if there's no decoded data left, decode next pack into {@code data}
     * 
     * @param in
     * @return
     */
    protected int readT(InputStream in) throws IOException {
      if (nextReadIndex == readIntTotalCount)
        return loadIntBatch(in);
      return data[nextReadIndex++];
    }
    
    @Override
    public int readInt(InputStream in) {
      try {
        return readT(in);
      } catch (IOException e) {
        LOG.warn("meet IOException when load batch from InputStream, return 0");
        return 0;
      }
    }

    /**
     * if remaining data has been run out, load next pack from InputStream
     * 
     * @param in
     * @throws IOException
     */
    protected int loadIntBatch(InputStream in) throws IOException {
      packNum = BytesUtils.readInt(in);
      packWidth = BytesUtils.readInt(in);
      count++;
      readHeader(in);

      encodingLength = ceil(packNum * packWidth);
      deltaBuf = BytesUtils.safeReadInputStreamToBytes(encodingLength, in);
      allocateDataArray();

      previous = firstValue;
      readIntTotalCount = packNum;
      nextReadIndex = 0;
      readPack();
      return firstValue;
    }

    private void readPack() throws IOException {
      for (int i = 0; i < packNum; i++) {
        readValue(i);
        previous = data[i];
      }
    }
    
    @Override
    protected void readHeader(InputStream in) throws IOException {
      minDeltaBase = BytesUtils.readInt(in);
      firstValue = BytesUtils.readInt(in);
    }

    @Override
    protected void allocateDataArray() {
      data = new int[packNum];
    }

    @Override
    protected void readValue(int i) {
      int v = BytesUtils.bytesToInt(deltaBuf, packWidth * i, packWidth);
      data[i] = previous + minDeltaBase + v;
    }
  }

  public static class LongDeltaDecoder extends DeltaBinaryDecoder {
	  private long firstValue;
	  private long [] data;
	  private long previous;
	  /**
	   * minimum value for all difference
	   */
	  private long minDeltaBase;
	  
    public LongDeltaDecoder() {
      super();
    }

    /**
     * if there's no decoded data left, decode next pack into {@code data}
     * 
     * @param in
     * @return
     */
    protected long readT(InputStream in) throws IOException {
      if (nextReadIndex == readIntTotalCount)
        return loadIntBatch(in);
      return data[nextReadIndex++];
    }
    /***
     * if remaining data has been run out, load next pack from InputStream
     * 
     * @param in
     * @throws IOException
     */
    protected long loadIntBatch(InputStream in) throws IOException {
      packNum = BytesUtils.readInt(in);
      packWidth = BytesUtils.readInt(in);
      count++;
      readHeader(in);

      encodingLength = ceil(packNum * packWidth);
      deltaBuf = BytesUtils.safeReadInputStreamToBytes(encodingLength, in);
      allocateDataArray();

      previous = firstValue;
      readIntTotalCount = packNum;
      nextReadIndex = 0;
      readPack();
      return firstValue;
    }
    

    private void readPack() throws IOException {
      for (int i = 0; i < packNum; i++) {
        readValue(i);
        previous = data[i];
      }
    }
    
    @Override
    public long readLong(InputStream in) {
      try {
        return readT(in);
      } catch (IOException e) {
        LOG.warn("meet IOException when load batch from InputStream, return 0");
        return 0;
      }
    }

    @Override
    protected void readHeader(InputStream in) throws IOException {
      minDeltaBase = BytesUtils.readLong(in);
      firstValue = BytesUtils.readLong(in);
    }

    @Override
    protected void allocateDataArray() {
      data = new long[packNum];
    }

    @Override
    protected void readValue(int i) {
      long v = BytesUtils.bytesToLong(deltaBuf, packWidth * i, packWidth);
      data[i] = previous + minDeltaBase + v;
    }

  }
}
