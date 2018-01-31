package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSChunkType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
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

    public void write(OutputStream outputStream) throws IOException {
        ReadWriteToBytesUtils.writeIsNull(measurementUID, outputStream);
        if(measurementUID != null)ReadWriteToBytesUtils.write(measurementUID, outputStream);

        ReadWriteToBytesUtils.writeIsNull(tsChunkType, outputStream);
        if(tsChunkType != null)ReadWriteToBytesUtils.write(tsChunkType.toString(), outputStream);

        ReadWriteToBytesUtils.write(fileOffset, outputStream);

        ReadWriteToBytesUtils.writeIsNull(compression, outputStream);
        if(compression != null)ReadWriteToBytesUtils.write(compression.toString(), outputStream);
    }

    public void read(InputStream inputStream) throws IOException {
        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            measurementUID = ReadWriteToBytesUtils.readString(inputStream);

        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            tsChunkType = TSChunkType.valueOf(ReadWriteToBytesUtils.readString(inputStream));

        fileOffset = ReadWriteToBytesUtils.readLong(inputStream);

        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            compression = CompressionTypeName.valueOf(ReadWriteToBytesUtils.readString(inputStream));
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
