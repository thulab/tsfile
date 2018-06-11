package cn.edu.tsinghua.tsfile.file.metadata;

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
}
