package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
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



    public int serializeTo(OutputStream outputStream) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteToBytesUtils.writeIsNull(deltaObjectID, outputStream);
        if(deltaObjectID != null)byteLen += ReadWriteToBytesUtils.write(deltaObjectID, outputStream);

        byteLen += ReadWriteToBytesUtils.write(totalByteSize, outputStream);
        byteLen += ReadWriteToBytesUtils.write(metadataOffset, outputStream);
        byteLen += ReadWriteToBytesUtils.write(metadataSize, outputStream);

        if(timeSeriesChunkMetaDataList == null){
            byteLen += ReadWriteToBytesUtils.write(0, outputStream);
        } else {
            byteLen += ReadWriteToBytesUtils.write(timeSeriesChunkMetaDataList.size(), outputStream);
            for(TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataList)
                byteLen += ReadWriteToBytesUtils.write(timeSeriesChunkMetaData, outputStream);
        }

        return byteLen;
    }


    public int serializeTo(ByteBuffer buffer) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteToBytesUtils.writeIsNull(deltaObjectID, buffer);
        if(deltaObjectID != null)byteLen += ReadWriteToBytesUtils.write(deltaObjectID, buffer);

        byteLen += ReadWriteToBytesUtils.write(totalByteSize, buffer);
        byteLen += ReadWriteToBytesUtils.write(metadataOffset, buffer);
        byteLen += ReadWriteToBytesUtils.write(metadataSize, buffer);

        if(timeSeriesChunkMetaDataList == null){
            byteLen += ReadWriteToBytesUtils.write(0, buffer);
        } else {
            byteLen += ReadWriteToBytesUtils.write(timeSeriesChunkMetaDataList.size(), buffer);
            for(TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataList)
                byteLen += ReadWriteToBytesUtils.write(timeSeriesChunkMetaData, buffer);
        }

        return byteLen;
    }

    public static RowGroupMetaData deserializeFrom(InputStream inputStream) throws IOException {
        RowGroupMetaData rowGroupMetaData = new RowGroupMetaData();

        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            rowGroupMetaData.setDeltaObjectID(ReadWriteToBytesUtils.readString(inputStream));

        rowGroupMetaData.setTotalByteSize(ReadWriteToBytesUtils.readLong(inputStream));
        rowGroupMetaData.setMetadataOffset(ReadWriteToBytesUtils.readLong(inputStream));
        rowGroupMetaData.setMetadataSize(ReadWriteToBytesUtils.readInt(inputStream));

        int size = ReadWriteToBytesUtils.readInt(inputStream);
        if(size > 0) {
            List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                timeSeriesChunkMetaDataList.add(ReadWriteToBytesUtils.readTimeSeriesChunkMetaData(inputStream));
            }
            rowGroupMetaData.setTimeSeriesChunkMetaDataList(timeSeriesChunkMetaDataList);
        }

        return rowGroupMetaData;
    }

    public static RowGroupMetaData deserializeFrom(ByteBuffer buffer) throws IOException {
        RowGroupMetaData rowGroupMetaData = new RowGroupMetaData();

        if(ReadWriteToBytesUtils.readIsNull(buffer))
            rowGroupMetaData.setDeltaObjectID(ReadWriteToBytesUtils.readString(buffer));

        rowGroupMetaData.setTotalByteSize(ReadWriteToBytesUtils.readLong(buffer));
        rowGroupMetaData.setMetadataOffset(ReadWriteToBytesUtils.readLong(buffer));
        rowGroupMetaData.setMetadataSize(ReadWriteToBytesUtils.readInt(buffer));

        int size = ReadWriteToBytesUtils.readInt(buffer);
        if(size > 0) {
            List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                timeSeriesChunkMetaDataList.add(ReadWriteToBytesUtils.readTimeSeriesChunkMetaData(buffer));
            }
            rowGroupMetaData.setTimeSeriesChunkMetaDataList(timeSeriesChunkMetaDataList);
        }

        return rowGroupMetaData;
    }
}
