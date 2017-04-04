package cn.edu.thu.tsfile.file.metadata.enums;

import cn.edu.thu.tsfile.common.exception.CompressionTypeNotSupportedException;
import cn.edu.thu.tsfile.format.CompressionType;

/**
 * @ClassName CompressionTypeName
 * @Description
 * @author XuYi xuyi556677@163.com
 * @date Apr 29, 2016 9:46:18 PM
 */
public enum CompressionTypeName {
  UNCOMPRESSED(CompressionType.UNCOMPRESSED, ""),
  SNAPPY(CompressionType.SNAPPY, ".snappy"),
  GZIP(CompressionType.GZIP, ".gz"),
  LZO(CompressionType.LZO, ".lzo"),
  SDT(CompressionType.SDT, ".sdt"),
  PAA(CompressionType.PAA, ".paa"),
  PLA(CompressionType.PLA, ".pla");

  public static CompressionTypeName fromConf(String name) {
    if (name == null) {
      return UNCOMPRESSED;
    }
    switch (name.trim().toUpperCase()) {
      case "UNCOMPRESSED":
        return UNCOMPRESSED;
      case "SNAPPY":
        return SNAPPY;
      case "GZIP":
        return GZIP;
      case "LZO":
        return LZO;
      case "SDT":
        return SDT;
      case "PAA":
        return PAA;
      case "PLA":
        return PLA;
      default:
        throw new CompressionTypeNotSupportedException(name);
    }
  }

  private final CompressionType tsfileCompressionType;
  private final String extension;

  private CompressionTypeName(CompressionType tsfileCompressionType, String extension) {
    this.tsfileCompressionType = tsfileCompressionType;
    this.extension = extension;
  }

  public CompressionType getTsfileCompressionCodec() {
    return tsfileCompressionType;
  }

  public String getExtension() {
    return extension;
  }
}