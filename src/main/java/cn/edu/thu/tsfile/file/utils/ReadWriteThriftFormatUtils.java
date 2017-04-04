package cn.edu.thu.tsfile.file.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import cn.edu.thu.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.thu.tsfile.file.metadata.statistics.Statistics;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.format.DataPageHeader;
import cn.edu.thu.tsfile.format.DictionaryPageHeader;
import cn.edu.thu.tsfile.format.Digest;
import cn.edu.thu.tsfile.format.Encoding;
import cn.edu.thu.tsfile.format.FileMetaData;
import cn.edu.thu.tsfile.format.PageHeader;
import cn.edu.thu.tsfile.format.PageType;

/**
 * 
 * ConverterUtils is a utility class. It provide conversion between tsfile and thrift metadata
 * class. It also provides function that read/write page header from/to stream
 * 
 * @author XuYi xuyi556677@163.com
 *
 */
public class ReadWriteThriftFormatUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReadWriteThriftFormatUtils.class);

  /**
   * write file metadata(thrift format) to stream
   * 
   * @param fileMetadata
   * @param to
   * @throws IOException
   */
  public static void writeFileMetaData(FileMetaData fileMetadata, OutputStream to)
      throws IOException {
    write(fileMetadata, to);
  }

  /**
   * read file metadata(thrift format) from stream
   * 
   * @param from
   * @throws IOException
   */
  public static FileMetaData readFileMetaData(InputStream from) throws IOException {
    return read(from, new FileMetaData());
  }

  /**
   * @Description write DataPageHeader to output stream. For more information about DataPageHeader,
   *              see PageHeader and
   *              DataPageHeader in tsfile-format
   * @return void
   * @throws IOException
   */
  public static void writeDataPageHeader(int uncompressedSize, int compressedSize, int numValues,
                                         Statistics<?> statistics, int numRows, TSEncoding encoding, OutputStream to,
                                         long max_timestamp, long min_timestamp) throws IOException {
    ReadWriteThriftFormatUtils.writePageHeader(newDataPageHeader(uncompressedSize, compressedSize,
        numValues, statistics, numRows, encoding, max_timestamp, min_timestamp), to);
  }

  /**
   * @Descriptioncreate a new PageHeader which contains DataPageHeader. For more information about
   *                    PageHeader and DataPageHeader, see PageHeader
   *                    and DataPageHeader in tsfile-format
   * @return PageHeader
   */
  private static PageHeader newDataPageHeader(int uncompressedSize, int compressedSize, int numValues,
      Statistics<?> statistics, int numRows, TSEncoding encoding, long max_timestamp,
      long min_timestamp) {
    PageHeader pageHeader = new PageHeader(PageType.DATA_PAGE, uncompressedSize, compressedSize);
    // TODO: pageHeader crc uncomplete

    pageHeader.setData_page_header(new DataPageHeader(numValues, numRows,
        Encoding.valueOf(encoding.toString()), max_timestamp, min_timestamp));
    if (!statistics.isEmpty()) {
      Digest digest = new Digest();
      digest.setMax(statistics.getMaxBytes());
      digest.setMin(statistics.getMinBytes());
      pageHeader.getData_page_header().setDigest(digest);
    }
    return pageHeader;
  }

  /**
   * @Description write DictionaryPageHeader to output stream. For more information about
   *              DictionaryPageHeader, see PageHeader and
   *              DictionaryPageHeader in tsfile-format.
   *              In current version, DictionaryPageHeader is not used.
   * @return void
   * @throws IOException
   */
  public void writeDictionaryPageHeader(int uncompressedSize, int compressedSize, int numValues,
      TSEncoding encoding, OutputStream to) throws IOException {
    PageHeader pageHeader =
        new PageHeader(PageType.DICTIONARY_PAGE, uncompressedSize, compressedSize);
    pageHeader.setDictionary_page_header(
        new DictionaryPageHeader(numValues, Encoding.valueOf(encoding.toString())));
    ReadWriteThriftFormatUtils.writePageHeader(pageHeader, to);
  }

  /**
   * write page header(thrift format) to stream
   * 
   * @param pageHeader
   * @param to
   * @throws IOException
   */
  public static void writePageHeader(PageHeader pageHeader, OutputStream to) throws IOException {
    write(pageHeader, to);
  }

  /**
   * read one page header from stream
   * 
   * @param from
   * @throws IOException
   */
  public static PageHeader readPageHeader(InputStream from) throws IOException {
    return read(from, new PageHeader());
  }

  private static void write(TBase<?, ?> tbase, OutputStream to) throws IOException {
    try {
      tbase.write(protocol(to));
    } catch (TException e) {
      LOGGER.error("tsfile-file Utils: can not write {}", tbase, e);
      throw new IOException(e);
    }
  }

  private static <T extends TBase<?, ?>> T read(InputStream from, T tbase) throws IOException {
    try {
      tbase.read(protocol(from));
      return tbase;
    } catch (TException e) {
      LOGGER.error("tsfile-file Utils: can not read {}", tbase, e);
      throw new IOException(e);
    }
  }

  private static TProtocol protocol(OutputStream to) {
    return new TCompactProtocol((new TIOStreamTransport(to)));
  }

  private static TProtocol protocol(InputStream from) {
    return new TCompactProtocol((new TIOStreamTransport(from)));
  }

}
