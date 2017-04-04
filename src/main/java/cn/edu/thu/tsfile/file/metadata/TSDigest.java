package cn.edu.thu.tsfile.file.metadata;

import java.nio.ByteBuffer;

import cn.edu.thu.tsfile.file.metadata.converter.IConverter;
import cn.edu.thu.tsfile.format.Digest;

/**
 * @Description For more information, see Digest in tsfile-format
 *              project
 * @author XuYi xuyi556677@163.com
 * @date Apr 29, 2016 9:54:41 PM
 */
public class TSDigest implements IConverter<Digest> {
  /**
   * Instead of long/double, we use ByteBuffer as type of max and min to improve versatility of
   * digest. Therefore, statistics of data whose type is int, long, double or flaot can be stored in digest.
   */
  public ByteBuffer max;
  public ByteBuffer min;

  public TSDigest() {}

  public TSDigest(ByteBuffer max, ByteBuffer min) {
    this.max = max;
    this.min = min;
  }

  @Override
  public String toString() {
    return String.format("max:%s, min:%s", max, min);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.corp.delta.tsfile.file.metadata.converter.IConverter#convertToTSF(java.lang.Object)
   */
  @Override
  public Digest convertToThrift() {
    Digest digest = new Digest();
    digest.setMax(max);
    digest.setMin(min);
    return digest;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.corp.delta.tsfile.file.metadata.converter.IConverter#convertToTSF(java.lang.Object)
   */
  @Override
  public void convertToTSF(Digest digestInThrift) {
    if(digestInThrift != null){
      this.max = digestInThrift.bufferForMax();
      this.min = digestInThrift.bufferForMin();
    }
  }
}
