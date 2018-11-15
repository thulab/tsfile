package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.common.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TsDeltaObjectMetadata {

    /**
     * size of RowGroupMetadataBlock in byte
     **/
    private int serializedSize = 2 * Long.BYTES + Integer.BYTES;// this field does not need to be serialized.

    /**
     * start time for a delta object
     **/
    private long startTime;

    /**
     * end time for a delta object
     **/
    private long endTime;

    /**
     * Row groups in this file
     */
    private List<RowGroupMetaData> rowGroupMetadataList = new ArrayList<>();


    public int getSerializedSize() {
        return serializedSize;
    }

    private void reCalculateSerializedSize() {
        serializedSize = 2 * Long.BYTES + // startTime , endTime
                Integer.BYTES; // size of rowGroupMetadataList

        for (RowGroupMetaData meta : rowGroupMetadataList) {
            serializedSize += meta.getSerializedSize();
        }
    }

    public TsDeltaObjectMetadata() {
    }

    /**
     * add row group metadata to rowGroups. THREAD NOT SAFE
     *
     * @param rowGroup - row group metadata to add
     */
    public void addRowGroupMetaData(RowGroupMetaData rowGroup) {
        rowGroupMetadataList.add(rowGroup);
        serializedSize += rowGroup.getSerializedSize();
    }

    public List<RowGroupMetaData> getRowGroups() {
        return Collections.unmodifiableList(rowGroupMetadataList);
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public int serializeTo(OutputStream outputStream) throws IOException {
        int byteLen = 0;
        byteLen += ReadWriteIOUtils.write(startTime, outputStream);
        byteLen += ReadWriteIOUtils.write(endTime, outputStream);

        if (rowGroupMetadataList == null) {
            byteLen += ReadWriteIOUtils.write(0, outputStream);
        } else {
            byteLen += ReadWriteIOUtils.write(rowGroupMetadataList.size(), outputStream);
            for (RowGroupMetaData rowGroupMetaData : rowGroupMetadataList)
                byteLen += ReadWriteIOUtils.write(rowGroupMetaData, outputStream);
        }

        assert getSerializedSize() == byteLen;
        return byteLen;
    }

    public int serializeTo(ByteBuffer buffer) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteIOUtils.write(startTime, buffer);
        byteLen += ReadWriteIOUtils.write(endTime, buffer);

        if (rowGroupMetadataList == null) {
            byteLen += ReadWriteIOUtils.write(0, buffer);
        } else {
            byteLen += ReadWriteIOUtils.write(rowGroupMetadataList.size(), buffer);
            for (RowGroupMetaData rowGroupMetaData : rowGroupMetadataList)
                byteLen += ReadWriteIOUtils.write(rowGroupMetaData, buffer);
        }

        return byteLen;
    }

    public static TsDeltaObjectMetadata deserializeFrom(InputStream inputStream) throws IOException {
        TsDeltaObjectMetadata deltaObjectMetadata = new TsDeltaObjectMetadata();

        deltaObjectMetadata.startTime = ReadWriteIOUtils.readLong(inputStream);
        deltaObjectMetadata.endTime = ReadWriteIOUtils.readLong(inputStream);

        int size = ReadWriteIOUtils.readInt(inputStream);
        if (size > 0) {
            List<RowGroupMetaData> rowGroupMetaDataList = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                rowGroupMetaDataList.add(ReadWriteIOUtils.readRowGroupMetaData(inputStream));
            }
            deltaObjectMetadata.rowGroupMetadataList = rowGroupMetaDataList;
        }

        deltaObjectMetadata.reCalculateSerializedSize();
        return deltaObjectMetadata;
    }

    public static TsDeltaObjectMetadata deserializeFrom(ByteBuffer buffer) throws IOException {
        TsDeltaObjectMetadata deltaObjectMetadata = new TsDeltaObjectMetadata();

        deltaObjectMetadata.startTime = ReadWriteIOUtils.readLong(buffer);
        deltaObjectMetadata.endTime = ReadWriteIOUtils.readLong(buffer);

        int size = ReadWriteIOUtils.readInt(buffer);
        if (size > 0) {
            List<RowGroupMetaData> rowGroupMetaDataList = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                rowGroupMetaDataList.add(ReadWriteIOUtils.readRowGroupMetaData(buffer));
            }
            deltaObjectMetadata.rowGroupMetadataList = rowGroupMetaDataList;
        }

        deltaObjectMetadata.reCalculateSerializedSize();
        return deltaObjectMetadata;
    }

    @Override
    public String toString() {
        return "TsDeltaObjectMetadata{" +
                "serializedSize=" + serializedSize +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", rowGroupMetadataList=" + rowGroupMetadataList +
                '}';
    }
}
