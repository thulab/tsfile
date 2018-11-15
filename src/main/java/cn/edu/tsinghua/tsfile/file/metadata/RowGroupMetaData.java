package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.common.utils.ReadWriteIOUtils;
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

    /**
     * Name of deltaObject, this field is not serialized.
     */
    private String deltaObjectID;

    /**
     * Byte size of this metadata. this field is not serialized.
     */
    private int serializedSize;

    /**
     * All time series chunks in this row group.
     */
    private List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList;

    public int getSerializedSize() {
        return serializedSize;
    }


    private RowGroupMetaData() {
        timeSeriesChunkMetaDataList = new ArrayList<>();
    }

    /**
     * @param deltaObjectID               name of deltaObject
     * @param timeSeriesChunkMetaDataList all time series chunks in this row group. Can not be Null.
     *                                    notice: after constructing a RowGroupMetadata instance. Donot use list.add()
     *                                    to modify `timeSeriesChunkMetaDataList`. Instead, use addTimeSeriesChunkMetaData()
     *                                    to make sure  getSerializedSize() is correct.
     */
    public RowGroupMetaData(String deltaObjectID, List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList) {
        assert timeSeriesChunkMetaDataList != null;
        this.deltaObjectID = deltaObjectID;
        this.timeSeriesChunkMetaDataList = timeSeriesChunkMetaDataList;
        reCalculateSerializedSize();
    }

    private void reCalculateSerializedSize(){
        serializedSize = Integer.BYTES + deltaObjectID.length() +
                Integer.BYTES; // size of timeSeriesChunkMetaDataList
        for (TimeSeriesChunkMetaData chunk : timeSeriesChunkMetaDataList) {
            serializedSize += chunk.getSerializedSize();
        }
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
        serializedSize += metadata.getSerializedSize();
    }

    public List<TimeSeriesChunkMetaData> getTimeSeriesChunkMetaDataList() {
        return timeSeriesChunkMetaDataList == null ? null
                : Collections.unmodifiableList(timeSeriesChunkMetaDataList);
    }


    public String toString() {
        return String.format(
                "RowGroupMetaData{ time series chunk list: %s }", timeSeriesChunkMetaDataList);
    }


    public String getDeltaObjectID() {
        return deltaObjectID;
    }

    public int serializeTo(OutputStream outputStream) throws IOException {
        int byteLen = 0;
        byteLen += ReadWriteIOUtils.write(deltaObjectID, outputStream);

        byteLen += ReadWriteIOUtils.write(timeSeriesChunkMetaDataList.size(), outputStream);
        for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataList)
            byteLen += ReadWriteIOUtils.write(timeSeriesChunkMetaData, outputStream);
        assert byteLen == getSerializedSize();
        return byteLen;
    }


    public int serializeTo(ByteBuffer buffer) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteIOUtils.write(deltaObjectID, buffer);

        byteLen += ReadWriteIOUtils.write(timeSeriesChunkMetaDataList.size(), buffer);
        for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataList)
            byteLen += ReadWriteIOUtils.write(timeSeriesChunkMetaData, buffer);
        assert byteLen == getSerializedSize();

        return byteLen;
    }

    public static RowGroupMetaData deserializeFrom(InputStream inputStream) throws IOException {
        RowGroupMetaData rowGroupMetaData = new RowGroupMetaData();

        rowGroupMetaData.deltaObjectID = ReadWriteIOUtils.readString(inputStream);

        int size = ReadWriteIOUtils.readInt(inputStream);
        rowGroupMetaData.serializedSize = Integer.BYTES + rowGroupMetaData.deltaObjectID.length() + Long.BYTES + Integer.BYTES;

        List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            TimeSeriesChunkMetaData metaData = ReadWriteIOUtils.readTimeSeriesChunkMetaData(inputStream);
            timeSeriesChunkMetaDataList.add(metaData);
            rowGroupMetaData.serializedSize += metaData.getSerializedSize();
        }
        rowGroupMetaData.timeSeriesChunkMetaDataList = timeSeriesChunkMetaDataList;


        return rowGroupMetaData;
    }

    public static RowGroupMetaData deserializeFrom(ByteBuffer buffer) throws IOException {
        RowGroupMetaData rowGroupMetaData = new RowGroupMetaData();

        rowGroupMetaData.deltaObjectID = (ReadWriteIOUtils.readString(buffer));

        int size = ReadWriteIOUtils.readInt(buffer);

        rowGroupMetaData.serializedSize = Integer.BYTES + rowGroupMetaData.deltaObjectID.length() + Long.BYTES + Integer.BYTES;


        List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            TimeSeriesChunkMetaData metaData = ReadWriteIOUtils.readTimeSeriesChunkMetaData(buffer);
            timeSeriesChunkMetaDataList.add(metaData);
            rowGroupMetaData.serializedSize += metaData.getSerializedSize();
        }
        rowGroupMetaData.timeSeriesChunkMetaDataList = timeSeriesChunkMetaDataList;

        return rowGroupMetaData;
    }


}
