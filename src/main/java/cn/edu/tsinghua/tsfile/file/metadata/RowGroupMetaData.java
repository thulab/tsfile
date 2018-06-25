package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * For more information, see RowGroupMetaData in cn.edu.thu.tsfile.format package
 */
public class RowGroupMetaData {
    private static final Logger LOGGER = LoggerFactory.getLogger(RowGroupMetaData.class);

    private String deltaObjectID;
    /**
     * Total byte size of all the uncompressed time series data in this row group
     */
    private long totalByteSize;

    /**
     *Byte offset of row group metadata in the file
     */
    private long metadataOffset;

    /**
     * Byte size of row group metadata.
     */
    private int metadataSize;

    private List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList;

    public RowGroupMetaData() {
        timeSeriesChunkMetaDataList = new ArrayList<TimeSeriesChunkMetaData>();
    }

    public RowGroupMetaData(String deltaObjectID, long totalByteSize, List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList) {
        this.deltaObjectID = deltaObjectID;
        this.totalByteSize = totalByteSize;
        this.timeSeriesChunkMetaDataList = timeSeriesChunkMetaDataList;
    }

    /**
     * add time series chunk metadata to list. THREAD NOT SAFE
     *
     * @param metadata time series metadata to add
     */
    public void addTimeSeriesChunkMetaData(TimeSeriesChunkMetaData metadata) {
        if (timeSeriesChunkMetaDataList == null) {
            timeSeriesChunkMetaDataList = new ArrayList<>();
        }
        timeSeriesChunkMetaDataList.add(metadata);
    }

    public List<TimeSeriesChunkMetaData> getMetaDatas() {
        return timeSeriesChunkMetaDataList == null ? null
                : Collections.unmodifiableList(timeSeriesChunkMetaDataList);
    }

    @Override
    public String toString() {
        return String.format(
                "RowGroupMetaData{ total byte size: %d, time series chunk list: %s }", totalByteSize, timeSeriesChunkMetaDataList);
    }

    public long getTotalByteSize() {
        return totalByteSize;
    }

    public void setTotalByteSize(long totalByteSize) {
        this.totalByteSize = totalByteSize;
    }

    public List<TimeSeriesChunkMetaData> getTimeSeriesChunkMetaDataList() {
        return timeSeriesChunkMetaDataList;
    }

    public void setTimeSeriesChunkMetaDataList(
            List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList) {
        this.timeSeriesChunkMetaDataList = timeSeriesChunkMetaDataList;
    }

    public String getDeltaObjectID() {
        return deltaObjectID;
    }

    public void setDeltaObjectID(String deltaObjectUID) {
        this.deltaObjectID = deltaObjectUID;
    }

    public long getMetadataOffset() {
        return metadataOffset;
    }

    public void setMetadataOffset(long metadataOffset) {
        this.metadataOffset = metadataOffset;
    }

    public int getMetadataSize() {
        return metadataSize;
    }

    public void setMetadataSize(int metadataSize) {
        this.metadataSize = metadataSize;
    }


    public int write(OutputStream outputStream) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteToBytesUtils.writeIsNull(deltaObjectID, outputStream);
        if(deltaObjectID != null)byteLen += ReadWriteToBytesUtils.write(deltaObjectID, outputStream);

        byteLen += ReadWriteToBytesUtils.write(numOfRows, outputStream);
        byteLen += ReadWriteToBytesUtils.write(totalByteSize, outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(path, outputStream);
        if(path != null)byteLen += ReadWriteToBytesUtils.write(path, outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(timeSeriesChunkMetaDataList, outputStream);
        if(timeSeriesChunkMetaDataList != null){
            byteLen += ReadWriteToBytesUtils.write(timeSeriesChunkMetaDataList.size(), outputStream);
            for(TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataList)
                byteLen += ReadWriteToBytesUtils.write(timeSeriesChunkMetaData, outputStream);
        }

        byteLen += ReadWriteToBytesUtils.writeIsNull(deltaObjectType, outputStream);
        if(deltaObjectType != null)byteLen += ReadWriteToBytesUtils.write(deltaObjectType, outputStream);

        return byteLen;
    }

    public void read(InputStream inputStream) throws IOException {
        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            deltaObjectID = ReadWriteToBytesUtils.readString(inputStream);

        numOfRows = ReadWriteToBytesUtils.readLong(inputStream);
        totalByteSize = ReadWriteToBytesUtils.readLong(inputStream);

        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            path = ReadWriteToBytesUtils.readString(inputStream);

        if(ReadWriteToBytesUtils.readIsNull(inputStream)){
            timeSeriesChunkMetaDataList = new ArrayList<>();

            int size = ReadWriteToBytesUtils.readInt(inputStream);
            for(int i = 0;i < size;i++) {
                timeSeriesChunkMetaDataList.add(ReadWriteToBytesUtils.readTimeSeriesChunkMetaData(inputStream));
            }
        }

        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            deltaObjectType = ReadWriteToBytesUtils.readString(inputStream);
    }
}
