package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSChunkType;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * store required members in TimeSeriesChunkMetaData
 */
public class TimeSeriesChunkProperties {
    private String measurementUID;

    /**
     * Type of this time series
     */
    @Deprecated
    private TSChunkType tsChunkType;

    /**
     * Byte offset in file_path to the RowGroupMetaData
     */
    private long fileOffset;
    private CompressionTypeName compression;

    public TimeSeriesChunkProperties() {
    }

    public TimeSeriesChunkProperties(String measurementUID, TSChunkType tsChunkType, long fileOffset,
                                     CompressionTypeName compression) {
        this.measurementUID = measurementUID;
        this.tsChunkType = tsChunkType;
        this.fileOffset = fileOffset;
        this.compression = compression;
    }

    public int write(OutputStream outputStream) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteToBytesUtils.writeIsNull(measurementUID, outputStream);
        if(measurementUID != null)byteLen += ReadWriteToBytesUtils.write(measurementUID, outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(tsChunkType, outputStream);
        if(tsChunkType != null)byteLen += ReadWriteToBytesUtils.write(tsChunkType.toString(), outputStream);

        byteLen += ReadWriteToBytesUtils.write(fileOffset, outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(compression, outputStream);
        if(compression != null)byteLen += ReadWriteToBytesUtils.write(compression.toString(), outputStream);

        return byteLen;
    }

    public void read(InputStream inputStream) throws IOException {
        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            measurementUID = ReadWriteToBytesUtils.readString(inputStream);

        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            tsChunkType = TSChunkType.valueOf(ReadWriteToBytesUtils.readString(inputStream));

        fileOffset = ReadWriteToBytesUtils.readLong(inputStream);

        if(ReadWriteToBytesUtils.readIsNull(inputStream)) {
            try {
                compression = CompressionTypeName.valueOf(ReadWriteToBytesUtils.readString(inputStream));
            }catch (Exception ex){
                System.out.println(ex.getMessage());
            }
        }
    }

    public TSChunkType getTsChunkType() {
        return tsChunkType;
    }

    public long getFileOffset() {
        return fileOffset;
    }

    public CompressionTypeName getCompression() {
        return compression;
    }

    public String getMeasurementUID() {
        return measurementUID;
    }

    @Override
    public String toString() {
        return String.format("measurementUID %s, TSChunkType %s, fileOffset %d, CompressionTypeName %s",
                measurementUID, tsChunkType, fileOffset, compression);
    }
}
