package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.converter.IConverter;
import cn.edu.tsinghua.tsfile.format.DeltaObject;

public class TsDeltaObject implements IConverter<DeltaObject> {
    /**
     * start position of RowGroupMetadataBlock in file
     **/
    public long offset;

    /**
     * size of RowGroupMetadataBlock in byte
     **/
    public int metadataBlockSize;

    /**
     * start time for a delta object
     **/
    public long startTime;

    /**
     * end time for a delta object
     **/
    public long endTime;

    /**
     * construct a TsDeltaObject
     * @param offset start position of RowGroupMetadataBlock in file
     * @param metadataBlockSize size of RowGroupMetadataBlock in byte
     * @param startTime start time for a delta object
     * @param endTime end time for a delta object
     */
    public TsDeltaObject(long offset, int metadataBlockSize, long startTime, long endTime) {
        this.offset = offset;
        this.metadataBlockSize = metadataBlockSize;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    /**
     * Serialize class TsDeltaObject to thrift format
     * @return class of thrift format
     */
    @Override
    public DeltaObject convertToThrift() {
        return new DeltaObject(offset, metadataBlockSize, startTime, endTime);
    }

    /**
     * Deserialize class TsDeltaObject from thrift format to normal format.
     * @param metadata thrift format of class TsDeltaObject
     */
    @Override
    public void convertToTSF(DeltaObject metadata) {
        this.offset = metadata.getOffset();
        this.metadataBlockSize = metadata.getMetadata_block_size();
        this.startTime = metadata.getStart_time();
        this.endTime = metadata.getEnd_time();
    }
}
