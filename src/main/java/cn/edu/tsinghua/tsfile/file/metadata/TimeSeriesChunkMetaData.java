package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSChunkType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;
import cn.edu.tsinghua.tsfile.format.CompressionType;
import cn.edu.tsinghua.tsfile.format.TimeSeriesChunkType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * For more information, see TimeSeriesChunkMetaData in cn.edu.thu.tsfile.format package
 */
public class TimeSeriesChunkMetaData {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesChunkMetaData.class);

    private TimeSeriesChunkProperties properties;

    private long numRows;

    /**
     * total byte size of all uncompressed pages in this time series chunk (including the headers)
     */
    private long totalByteSize;

    /**
     * Optional json metadata
     */
    private List<String> jsonMetaData;

    /**
     * Byte offset from beginning of file to first data page
     */
    private long dataPageOffset;

    /**
     * Byte offset from beginning of file to root index page
     */
    private long indexPageOffset;

    /**
     * Byte offset from the beginning of file to first (only) dictionary page
     */
    private long dictionaryPageOffset;

    /**
     * one of TSeriesMetaData and VSeriesMetaData is not null
     */
    private TInTimeSeriesChunkMetaData tInTimeSeriesChunkMetaData;
    private VInTimeSeriesChunkMetaData vInTimeSeriesChunkMetaData;

    public TimeSeriesChunkMetaData() {
        properties = new TimeSeriesChunkProperties();
        jsonMetaData = new ArrayList<String>();
    }

    public TimeSeriesChunkMetaData(String measurementUID, TSChunkType tsChunkGroup, long fileOffset,
                                   CompressionTypeName compression) {
        this();
        this.properties = new TimeSeriesChunkProperties(measurementUID, tsChunkGroup, fileOffset, compression);
    }

    public TimeSeriesChunkProperties getProperties() {
        return properties;
    }

    public void setProperties(TimeSeriesChunkProperties properties) {
        this.properties = properties;
    }

    public int write(OutputStream outputStream) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteToBytesUtils.writeIsNull(properties, outputStream);
        if(properties != null)byteLen += ReadWriteToBytesUtils.write(properties, outputStream);

        byteLen += ReadWriteToBytesUtils.write(numRows, outputStream);
        byteLen += ReadWriteToBytesUtils.write(totalByteSize, outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(jsonMetaData, outputStream);
        if(jsonMetaData != null)byteLen += ReadWriteToBytesUtils.write(jsonMetaData, TSDataType.TEXT, outputStream);

        byteLen += ReadWriteToBytesUtils.write(dataPageOffset, outputStream);
        byteLen += ReadWriteToBytesUtils.write(indexPageOffset, outputStream);
        byteLen += ReadWriteToBytesUtils.write(dictionaryPageOffset, outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(tInTimeSeriesChunkMetaData, outputStream);
        if(tInTimeSeriesChunkMetaData != null)byteLen += ReadWriteToBytesUtils.write(tInTimeSeriesChunkMetaData, outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(vInTimeSeriesChunkMetaData, outputStream);
        if(vInTimeSeriesChunkMetaData != null)byteLen += ReadWriteToBytesUtils.write(vInTimeSeriesChunkMetaData, outputStream);

        return byteLen;
    }

    public void read(InputStream inputStream) throws IOException {
        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            properties = ReadWriteToBytesUtils.readTimeSeriesChunkProperties(inputStream);

        numRows = ReadWriteToBytesUtils.readLong(inputStream);
        totalByteSize = ReadWriteToBytesUtils.readLong(inputStream);

        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            jsonMetaData = ReadWriteToBytesUtils.readStringList(inputStream);

        dataPageOffset = ReadWriteToBytesUtils.readLong(inputStream);
        indexPageOffset = ReadWriteToBytesUtils.readLong(inputStream);
        dictionaryPageOffset = ReadWriteToBytesUtils.readLong(inputStream);

        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            tInTimeSeriesChunkMetaData = ReadWriteToBytesUtils.readTInTimeSeriesChunkMetaData(inputStream);

        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            vInTimeSeriesChunkMetaData = ReadWriteToBytesUtils.readVInTimeSeriesChunkMetaData(inputStream);
    }

    private cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData initTimeSeriesChunkMetaDataInThrift() {
        cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData metadataInThrift =
                new cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData(
                        properties.getMeasurementUID(),
                        properties.getTsChunkType() == null ? null : TimeSeriesChunkType.valueOf(properties.getTsChunkType().toString()),
                        properties.getFileOffset(),
                        properties.getCompression() == null ? null : CompressionType.valueOf(properties.getCompression().toString()));
        metadataInThrift.setNum_rows(numRows);
        metadataInThrift.setTotal_byte_size(totalByteSize);
        metadataInThrift.setJson_metadata(jsonMetaData);
        metadataInThrift.setData_page_offset(dataPageOffset);
        metadataInThrift.setIndex_page_offset(indexPageOffset);
        metadataInThrift.setDictionary_page_offset(dictionaryPageOffset);
        return metadataInThrift;
    }

    private void initTimeSeriesChunkMetaDataInTSFile(
            cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData metadataInThrift) {
        properties = new TimeSeriesChunkProperties(
                metadataInThrift.getMeasurement_uid(),
                metadataInThrift.getTimeseries_chunk_type() == null ? null : TSChunkType.valueOf(metadataInThrift.getTimeseries_chunk_type().toString()),
                metadataInThrift.getFile_offset(),
                metadataInThrift.getCompression_type() == null ? null : CompressionTypeName.valueOf(metadataInThrift.getCompression_type().toString()));
        numRows = metadataInThrift.getNum_rows();
        totalByteSize = metadataInThrift.getTotal_byte_size();
        jsonMetaData = metadataInThrift.getJson_metadata();
        dataPageOffset = metadataInThrift.getData_page_offset();
        indexPageOffset = metadataInThrift.getIndex_page_offset();
        dictionaryPageOffset = metadataInThrift.getDictionary_page_offset();
    }

    @Override
    public String toString() {
        return String.format(
                "TimeSeriesChunkProperties %s, numRows %d, totalByteSize %d, jsonMetaData %s, dataPageOffset %d, indexPageOffset %d, dictionaryPageOffset %s",
                properties, numRows, totalByteSize, jsonMetaData, dataPageOffset, indexPageOffset,
                dictionaryPageOffset);
    }

    public long getNumRows() {
        return numRows;
    }

    public void setNumRows(long numRows) {
        this.numRows = numRows;
    }

    public long getTotalByteSize() {
        return totalByteSize;
    }

    public void setTotalByteSize(long totalByteSize) {
        this.totalByteSize = totalByteSize;
    }

    public List<String> getJsonMetaData() {
        return jsonMetaData;
    }

    public void setJsonMetaData(List<String> jsonMetaData) {
        this.jsonMetaData = jsonMetaData;
    }

    public long getDataPageOffset() {
        return dataPageOffset;
    }

    public void setDataPageOffset(long dataPageOffset) {
        this.dataPageOffset = dataPageOffset;
    }

    public long getIndexPageOffset() {
        return indexPageOffset;
    }

    public void setIndexPageOffset(long indexPageOffset) {
        this.indexPageOffset = indexPageOffset;
    }

    public long getDictionaryPageOffset() {
        return dictionaryPageOffset;
    }

    public void setDictionaryPageOffset(long dictionaryPageOffset) {
        this.dictionaryPageOffset = dictionaryPageOffset;
    }

    public TInTimeSeriesChunkMetaData getTInTimeSeriesChunkMetaData() {
        return tInTimeSeriesChunkMetaData;
    }

    public void setTInTimeSeriesChunkMetaData(TInTimeSeriesChunkMetaData tInTimeSeriesChunkMetaData) {
        this.tInTimeSeriesChunkMetaData = tInTimeSeriesChunkMetaData;
    }

    public VInTimeSeriesChunkMetaData getVInTimeSeriesChunkMetaData() {
        return vInTimeSeriesChunkMetaData;
    }

    public void setVInTimeSeriesChunkMetaData(VInTimeSeriesChunkMetaData vInTimeSeriesChunkMetaData) {
        this.vInTimeSeriesChunkMetaData = vInTimeSeriesChunkMetaData;
    }
}
