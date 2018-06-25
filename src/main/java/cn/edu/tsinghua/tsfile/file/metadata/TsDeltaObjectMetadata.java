package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TsDeltaObjectMetadata {
    /**
     * start position of RowGroupMetadataBlock in file
     **/
    private long offset;

    /**
     * size of RowGroupMetadataBlock in byte
     **/
    private int metadataBlockSize;

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
    private List<RowGroupMetaData> rowGroupMetadataList;

    public TsDeltaObjectMetadata(){
    }

    /**
     * add row group metadata to rowGroups. THREAD NOT SAFE
     *
     * @param rowGroup - row group metadata to add
     */
    public void addRowGroupMetaData(RowGroupMetaData rowGroup) {
        if (rowGroupMetadataList == null) {
            rowGroupMetadataList = new ArrayList<RowGroupMetaData>();
        }
        rowGroupMetadataList.add(rowGroup);
    }

    public List<RowGroupMetaData> getRowGroups() {
        return rowGroupMetadataList;
    }

    public void setRowGroups(List<RowGroupMetaData> rowGroupMetadataList) {
        this.rowGroupMetadataList = rowGroupMetadataList;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public int getMetadataBlockSize() {
        return metadataBlockSize;
    }

    public void setMetadataBlockSize(int metadataBlockSize) {
        this.metadataBlockSize = metadataBlockSize;
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

    public int serialize(OutputStream outputStream) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteToBytesUtils.write(offset, outputStream);
        byteLen += ReadWriteToBytesUtils.write(metadataBlockSize, outputStream);
        byteLen += ReadWriteToBytesUtils.write(startTime, outputStream);
        byteLen += ReadWriteToBytesUtils.write(endTime, outputStream);

        if(rowGroupMetadataList == null){
            byteLen += ReadWriteToBytesUtils.write(0, outputStream);
        } else {
            byteLen += ReadWriteToBytesUtils.write(rowGroupMetadataList.size(), outputStream);
            for(RowGroupMetaData rowGroupMetaData : rowGroupMetadataList)
                byteLen += ReadWriteToBytesUtils.write(rowGroupMetaData, outputStream);
        }

        return byteLen;
    }

    public int serialize(ByteBuffer buffer) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteToBytesUtils.write(offset, buffer);
        byteLen += ReadWriteToBytesUtils.write(metadataBlockSize, buffer);
        byteLen += ReadWriteToBytesUtils.write(startTime, buffer);
        byteLen += ReadWriteToBytesUtils.write(endTime, buffer);

        if(rowGroupMetadataList == null){
            byteLen += ReadWriteToBytesUtils.write(0, buffer);
        } else {
            byteLen += ReadWriteToBytesUtils.write(rowGroupMetadataList.size(), buffer);
            for(RowGroupMetaData rowGroupMetaData : rowGroupMetadataList)
                byteLen += ReadWriteToBytesUtils.write(rowGroupMetaData, buffer);
        }

        return byteLen;
    }

    public static TsDeltaObjectMetadata deserialize(InputStream inputStream) throws IOException {
        TsDeltaObjectMetadata deltaObjectMetadata = new TsDeltaObjectMetadata();

        deltaObjectMetadata.offset = ReadWriteToBytesUtils.readLong(inputStream);
        deltaObjectMetadata.metadataBlockSize = ReadWriteToBytesUtils.readInt(inputStream);
        deltaObjectMetadata.startTime = ReadWriteToBytesUtils.readLong(inputStream);
        deltaObjectMetadata.endTime = ReadWriteToBytesUtils.readLong(inputStream);

        int size = ReadWriteToBytesUtils.readInt(inputStream);
        if(size > 0) {
            List<RowGroupMetaData> rowGroupMetaDataList = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                rowGroupMetaDataList.add(ReadWriteToBytesUtils.readRowGroupMetaData(inputStream));
            }
            deltaObjectMetadata.rowGroupMetadataList = rowGroupMetaDataList;
        }

        return deltaObjectMetadata;
    }

    public static TsDeltaObjectMetadata deserialize(ByteBuffer buffer) throws IOException {
        TsDeltaObjectMetadata deltaObjectMetadata = new TsDeltaObjectMetadata();

        deltaObjectMetadata.offset = ReadWriteToBytesUtils.readLong(buffer);
        deltaObjectMetadata.metadataBlockSize = ReadWriteToBytesUtils.readInt(buffer);
        deltaObjectMetadata.startTime = ReadWriteToBytesUtils.readLong(buffer);
        deltaObjectMetadata.endTime = ReadWriteToBytesUtils.readLong(buffer);

        int size = ReadWriteToBytesUtils.readInt(buffer);
        if(size > 0) {
            List<RowGroupMetaData> rowGroupMetaDataList = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                rowGroupMetaDataList.add(ReadWriteToBytesUtils.readRowGroupMetaData(buffer));
            }
            deltaObjectMetadata.rowGroupMetadataList = rowGroupMetaDataList;
        }

        return deltaObjectMetadata;
    }
}
