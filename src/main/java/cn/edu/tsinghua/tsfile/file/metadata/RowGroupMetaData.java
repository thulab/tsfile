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

    private String deltaObjectID; //TODO 这个应该放到DeltaObjectMetadata中去
    /**
     * Total serialized byte size of this row group data (including the rowgroup header)
     */
    private long totalByteSize;

    /**
     * Byte offset of the corresponding data in the file
     * Notice:  include the rowgroup header
     */
    private long fileOffsetOfCorrespondingData;

    /**
     * Byte offset of row group metadata in the file
     */
   // private long metadataOffset; // this field is not serialized.

    /**
     * Byte size of this metadata.
     */
    private int serializedSize; // this field is not serialized.

    private int sizeOfChunkList;// this field is to check whether users call list.add() to modify the list
    // rather than addTimeSeriesChunkMetaData()

    private List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList;

    public int getSerializedSize() {
        if( sizeOfChunkList!=timeSeriesChunkMetaDataList.size()){
            reCalculateSerializedSize();
        }
        return serializedSize;
    }


    private RowGroupMetaData() {
        timeSeriesChunkMetaDataList = new ArrayList<TimeSeriesChunkMetaData>();
    }

    /**
     * @param deltaObjectID
     * @param totalByteSize
     * @param timeSeriesChunkMetaDataList Can not be Null.
     *                                    notice: after constructing a RowGroupMetadata instance. Donot use list.add()
     *                                    to modify `timeSeriesChunkMetaDataList`. Instead, use addTimeSeriesChunkMetaData()
     *                                    to make sure  getSerializedSize() is correct.
     */
    public RowGroupMetaData(String deltaObjectID, long totalByteSize, long fileOffsetOfCorrespondingData, List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList) {
        assert timeSeriesChunkMetaDataList != null;
        this.deltaObjectID = deltaObjectID;
        this.totalByteSize = totalByteSize;
        this.fileOffsetOfCorrespondingData=fileOffsetOfCorrespondingData;
        this.timeSeriesChunkMetaDataList = timeSeriesChunkMetaDataList;
        reCalculateSerializedSize();
    }

    private void reCalculateSerializedSize(){
        serializedSize = Integer.BYTES + deltaObjectID.length() + 2 * Long.BYTES + Integer.BYTES;
        for (TimeSeriesChunkMetaData chunk : timeSeriesChunkMetaDataList) {
            serializedSize += chunk.getSerializedSize();
        }
        this.sizeOfChunkList = timeSeriesChunkMetaDataList.size();
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
        sizeOfChunkList++;
    }

    public List<TimeSeriesChunkMetaData> getTimeSeriesChunkMetaDataList() {
        return timeSeriesChunkMetaDataList == null ? null
                : Collections.unmodifiableList(timeSeriesChunkMetaDataList);
    }

    @Override
    public String toString() {
        return String.format(
                "RowGroupMetaData{ total byte size: %d, time series chunk list: %s }", totalByteSize, timeSeriesChunkMetaDataList);
    }

    /**
     * @return  Total serialized byte size of this row group data (including the rowgroup header)
     */
    public long getTotalByteSize() {
        return totalByteSize;
    }

    public void setTotalByteSize(long totalByteSize) {//TODO 删除掉比较保险
        this.totalByteSize = totalByteSize;
    }

//    public List<TimeSeriesChunkMetaData> getTimeSeriesChunkMetaDataList() {
//        return timeSeriesChunkMetaDataList;
//    }

//    private void setTimeSeriesChunkMetaDataList(
//            List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList) {
//        this.timeSeriesChunkMetaDataList = timeSeriesChunkMetaDataList;
//    }

    public String getDeltaObjectID() {
        return deltaObjectID;
    }

//    public void setDeltaObjectID(String deltaObjectUID) {
//        this.deltaObjectID = deltaObjectUID;
//    }

//    public long getMetadataOffset() {
//        return metadataOffset;
//    }

//    public void setMetadataOffset(long metadataOffset) {
//        this.metadataOffset = metadataOffset;
//    }


    public int serializeTo(OutputStream outputStream) throws IOException {
        if (sizeOfChunkList != timeSeriesChunkMetaDataList.size()) {
            //someone call list.add() method rather than using addTimeSeriesChunkMetaData(), so that we have to recalculate
            //the serializedSize.
            reCalculateSerializedSize();
        }
        int byteLen = 0;
        byteLen += ReadWriteIOUtils.write(deltaObjectID, outputStream);
        byteLen += ReadWriteIOUtils.write(totalByteSize, outputStream);
        byteLen += ReadWriteIOUtils.write(fileOffsetOfCorrespondingData, outputStream);

        //byteLen += ReadWriteIOUtils.write(metadataOffset, outputStream);
        //byteLen += ReadWriteIOUtils.write(metadataSize, outputStream);

//        if(timeSeriesChunkMetaDataList == null){
//            byteLen += ReadWriteIOUtils.write(0, outputStream);
//        } else {
        byteLen += ReadWriteIOUtils.write(timeSeriesChunkMetaDataList.size(), outputStream);
        for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataList)
            byteLen += ReadWriteIOUtils.write(timeSeriesChunkMetaData, outputStream);
//        }
        assert byteLen == getSerializedSize();
        return byteLen;
    }


    public int serializeTo(ByteBuffer buffer) throws IOException {
        if (sizeOfChunkList != timeSeriesChunkMetaDataList.size()) {
            //someone call list.add() method rather than using addTimeSeriesChunkMetaData(), so that we have to recalculate
            //the serializedSize.
            reCalculateSerializedSize();
        }
        int byteLen = 0;

        byteLen += ReadWriteIOUtils.write(deltaObjectID, buffer);

        byteLen += ReadWriteIOUtils.write(totalByteSize, buffer);
        byteLen += ReadWriteIOUtils.write(fileOffsetOfCorrespondingData, buffer);
        //byteLen += ReadWriteIOUtils.write(metadataOffset, buffer);
        //byteLen += ReadWriteIOUtils.write(metadataSize, buffer);


        byteLen += ReadWriteIOUtils.write(timeSeriesChunkMetaDataList.size(), buffer);
        for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataList)
            byteLen += ReadWriteIOUtils.write(timeSeriesChunkMetaData, buffer);
        assert byteLen == getSerializedSize();

        return byteLen;
    }

    public static RowGroupMetaData deserializeFrom(InputStream inputStream) throws IOException {
        RowGroupMetaData rowGroupMetaData = new RowGroupMetaData();

        rowGroupMetaData.deltaObjectID = ReadWriteIOUtils.readString(inputStream);

        rowGroupMetaData.totalByteSize = ReadWriteIOUtils.readLong(inputStream);
        rowGroupMetaData.fileOffsetOfCorrespondingData = ReadWriteIOUtils.readLong(inputStream);

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

        rowGroupMetaData.totalByteSize = (ReadWriteIOUtils.readLong(buffer));

        rowGroupMetaData.fileOffsetOfCorrespondingData = ReadWriteIOUtils.readLong(buffer);

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

    public long getFileOffsetOfCorrespondingData() {
        return fileOffsetOfCorrespondingData;
    }
}
