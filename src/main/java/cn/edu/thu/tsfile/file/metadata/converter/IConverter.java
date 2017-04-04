package cn.edu.thu.tsfile.file.metadata.converter;

/**
 * @Description: convert metadata between TSFile format and thrift format
 * @author XuYi xuyi556677@163.com
 * @date Apr 29, 2016 9:51:09 PM
 * 
 * @param <T>
 */
public interface IConverter<T> {
  /**
   * @Description convert TSFile format metadata to thrift format
   * @param
   * @return T - metadata in thrift format
   */
  public T convertToThrift();


  /**
   * @Description convert thrift format metadata to TSFile format
   * @param metadata - metadata in thrift format
   * @return void
   */
  public void convertToTSF(T metadata);
}
