package cn.edu.thu.tsfile.file.metadata.converter;

import cn.edu.thu.tsfile.file.metadata.TSFileMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.format.FileMetaData;

/**
 * @Description converter for file metadata
 * @author XuYi xuyi556677@163.com
 * @date Apr 29, 2016 10:06:10 PM
 */
public class TSFileMetaDataConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(TSFileMetaDataConverter.class);

  /**
   * @Description convert tsfile format file matadata to thrift format file matadata. For more
   *              information about thrift format file matadata, see
   *              FileMetaData in tsfile-format project
   * @param tsfMetadata - file metadata in tsfile format
   * @return FileMetaData - file metadata in thrift format
   */
  public FileMetaData toThriftFileMetadata(TSFileMetaData fileMetadataInTSFile) {
    try {
      return fileMetadataInTSFile.convertToThrift();
    } catch (Exception e) {
      LOGGER.error(
          "tsfile-file TSFileMetaDataConverter: failed to convert metadata from TSFile to thrift, content is {}",
          fileMetadataInTSFile, e);
    }
    return null;
  }

  /**
   * @Description convert thrift format file matadata to tsfile format file matadata. For more
   *              information about thrift format file matadata, see
   *              FileMetaData in tsfile-format
   * @param fileMetaData - file metadata in thrift format
   * @return TSFMetaData - file metadata in tsfile format
   */
  public TSFileMetaData toTSFileMetadata(FileMetaData fileMetaDataInThrift) {
    TSFileMetaData fileMetaDataInTSFile = new TSFileMetaData();
    try {
      fileMetaDataInTSFile.convertToTSF(fileMetaDataInThrift);
    } catch (Exception e) {
      LOGGER.error(
          "tsfile-file TSFileMetaDataConverter: failed to convert metadata from thrift to TSFile, content is {}",
          fileMetaDataInThrift, e);
    }
    return fileMetaDataInTSFile;
  }

  public int caculatePageHeaderSize(int digestSize){
    //PageHeader: PageType--4, uncompressedSize--4,compressedSize--4
    //DatapageHeader: numValues--4, numNulls--4, numRows--4, Encoding--4, isCompressed--1, maxTimestamp--8, minTimestamp--8
    //Digest: max ByteBuffer, min ByteBuffer
    // * 2 to caculate max object size in memory

    return 2*(45+digestSize);
  }
}
