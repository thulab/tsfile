package cn.edu.tsinghua.tsfile.timeseries.write.io;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import cn.edu.tsinghua.tsfile.file.metadata.*;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteByteStreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.constant.StatisticConstant;
import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.common.utils.ListByteArrayOutputStream;
import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.TsDeltaObjectMetadata;
import cn.edu.tsinghua.tsfile.file.metadata.converter.TsFileMetaDataConverter;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.statistics.Statistics;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;

/**
 * TSFileIOWriter is used to construct metadata and write data stored in memory
 * to output stream.
 *
 * @author kangrong
 */
public class TsFileIOWriter {

    public static final byte[] magicStringBytes;
    public static final TsFileMetaDataConverter metadataConverter = new TsFileMetaDataConverter();
    private static final Logger LOG = LoggerFactory.getLogger(TsFileIOWriter.class);

    static {
        magicStringBytes = BytesUtils.StringToBytes(TSFileConfig.MAGIC_STRING);
    }

    private ITsRandomAccessFileWriter out;
    protected List<RowGroupMetaData> rowGroupMetaDatas = new ArrayList<>();
    private RowGroupMetaData currentRowGroupMetaData;
    private TimeSeriesChunkMetaData currentChunkMetaData;


    public TsFileIOWriter() {

    }

    public void setIOWriter(ITsRandomAccessFileWriter out) {
        this.out = out;
    }

    /**
     * for writing a new tsfile.
     *
     * @param file be used to output written data
     * @throws IOException if I/O error occurs
     */
    public TsFileIOWriter(File file) throws IOException {
        this.out = new TsRandomAccessFileWriter(file);
        startFile();
    }

    /**
     * for writing a new tsfile.
     *
     * @param output be used to output written data
     * @throws IOException if I/O error occurs
     */
    public TsFileIOWriter(ITsRandomAccessFileWriter output) throws IOException {
        this.out = output;
        startFile();
    }

    /**
     * This is just used to restore one TSFile from List of RowGroupMetaData and
     * the offset.
     *
     * @param output    be used to output written data
     * @param offset    offset to restore
     * @param rowGroups given a constructed row group list for fault recovery
     * @throws IOException if I/O error occurs
     */
    public TsFileIOWriter(ITsRandomAccessFileWriter output, long offset, List<RowGroupMetaData> rowGroups)
            throws IOException {
        this.out = output;
        out.seek(offset);
        this.rowGroupMetaDatas = rowGroups;
    }

    /**
     * Writes given <code>ListByteArrayOutputStream</code> to output stream.
     * This method is called when total memory size exceeds the row group size
     * threshold.
     *
     * @param bytes - data of several pages which has been packed
     * @throws IOException if an I/O error occurs.
     */
    public void writeBytesToStream(ListByteArrayOutputStream bytes) throws IOException {
        bytes.writeAllTo(out.getOutputStream());
    }

    private void startFile() throws IOException {
        out.write(magicStringBytes);
    }

    /**
     * start a {@linkplain RowGroupMetaData RowGroupMetaData}.
     *
     * @param recordCount   - the record count of this time series input in this stage
     */

    public void startRowGroup(String deltaObjectId) {
        LOG.debug("start row group:{}", deltaObjectId);
        currentRowGroupMetaData = new RowGroupMetaData(deltaObjectId,  0, new ArrayList<>());
    }

    /**
     * start a {@linkplain TimeSeriesChunkMetaData TimeSeriesChunkMetaData}.
     *
     * @param descriptor           - measurement of this time series
     * @param compressionCodecName - compression name of this time series
     * @param tsDataType           - data type
     * @param statistics           - statistic of the whole series
     * @param maxTime              - maximum timestamp of the whole series in this stage
     * @param minTime              - minimum timestamp of the whole series in this stage
     * @throws IOException if I/O error occurs
     */
    public void startSeries(MeasurementDescriptor descriptor, CompressionTypeName compressionCodecName,
                            TSDataType tsDataType, Statistics<?> statistics, long maxTime, long minTime) throws IOException {
        LOG.debug("start series:{}", descriptor);
        currentChunkMetaData = new TimeSeriesChunkMetaData(descriptor.getMeasurementId(), out.getPos(), compressionCodecName, tsDataType, minTime, maxTime);
        TsDigest tsDigest = new TsDigest();
        Map<String, ByteBuffer> statisticsMap = new HashMap<>();
        // TODO add your statistics
        statisticsMap.put(StatisticConstant.MAX_VALUE, ByteBuffer.wrap(statistics.getMaxBytes()));
        statisticsMap.put(StatisticConstant.MIN_VALUE, ByteBuffer.wrap(statistics.getMinBytes()));
        statisticsMap.put(StatisticConstant.FIRST, ByteBuffer.wrap(statistics.getFirstBytes()));
        statisticsMap.put(StatisticConstant.SUM, ByteBuffer.wrap(statistics.getSumBytes()));
        statisticsMap.put(StatisticConstant.LAST, ByteBuffer.wrap(statistics.getLastBytes()));
        tsDigest.setStatistics(statisticsMap);

        currentChunkMetaData.setDigest(tsDigest);
    }

    public void endSeries(long size, long totalValueCount) {
        LOG.debug("end series:{},totalvalue:{}", currentChunkMetaData, totalValueCount);
        currentChunkMetaData.setTotalByteSize(size);
        currentChunkMetaData.setNumOfPoints(totalValueCount);
        currentRowGroupMetaData.addTimeSeriesChunkMetaData(currentChunkMetaData);
        currentChunkMetaData = null;
    }

    public void endRowGroup(long memSize) {
        currentRowGroupMetaData.setTotalByteSize(memSize);
        rowGroupMetaDatas.add(currentRowGroupMetaData);
        LOG.debug("end row group:{}", currentRowGroupMetaData);
        currentRowGroupMetaData = null;
    }

    /**
     * write {@linkplain TsFileMetaData TSFileMetaData} to output stream and
     * close it.
     *
     * @param schema FileSchema
     * @throws IOException if I/O error occurs
     */
    public void endFile(FileSchema schema) throws IOException {
        List<TimeSeriesMetadata> timeSeriesList = schema.getTimeSeriesMetadatas();
        LOG.debug("get time series list:{}", timeSeriesList);
        // clustering rowGroupMetadata and build the range

        String currentDeltaObject;
        TsDeltaObjectMetadata currentTsDeltaObjectMetadata;

        LinkedHashMap<String, TsDeltaObjectMetadata> tsDeltaObjectMetadataMap = new LinkedHashMap<>();
        for (RowGroupMetaData rowGroupMetaData : rowGroupMetaDatas) {
            currentDeltaObject = rowGroupMetaData.getDeltaObjectID();
            if (!tsDeltaObjectMetadataMap.containsKey(currentDeltaObject)) {
                TsDeltaObjectMetadata tsDeltaObjectMetadata = new TsDeltaObjectMetadata();
                tsDeltaObjectMetadataMap.put(currentDeltaObject, tsDeltaObjectMetadata);
            }
            tsDeltaObjectMetadataMap.get(currentDeltaObject).addRowGroupMetaData(rowGroupMetaData);
        }
        Iterator<Map.Entry<String, TsDeltaObjectMetadata>> iterator = tsDeltaObjectMetadataMap.entrySet().iterator();

        /** start time for a delta object **/
        long startTime;

        /** end time for a delta object **/
        long endTime;

        while (iterator.hasNext()) {
            startTime = Long.MAX_VALUE;
            endTime = Long.MIN_VALUE;

            Map.Entry<String, TsDeltaObjectMetadata> entry = iterator.next();
            currentTsDeltaObjectMetadata = entry.getValue();

            for (RowGroupMetaData rowGroupMetaData : currentTsDeltaObjectMetadata.getRowGroups()) {
                for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : rowGroupMetaData
                        .getTimeSeriesChunkMetaDataList()) {
                    startTime = Long.min(startTime, timeSeriesChunkMetaData.getStartTime());
                    endTime = Long.max(endTime, timeSeriesChunkMetaData.getEndTime());
                }
            }
            // flush tsRowGroupBlockMetaDatas in order
            ReadWriteByteStreamUtils.writeRowGroupBlockMetadata(currentTsDeltaObjectMetadata, out);
            currentTsDeltaObjectMetadata.setStartTime(startTime);
            currentTsDeltaObjectMetadata.setEndTime(endTime);
        }

        TsFileMetaData tsFileMetaData = new TsFileMetaData(tsDeltaObjectMetadataMap, timeSeriesList,
                TSFileConfig.currentVersion);
        serializeTsFileMetadata(tsFileMetaData);
        out.close();
        LOG.info("output stream is closed");
    }

    /**
     * get the length of normal OutputStream.
     *
     * @return - length of normal OutputStream
     * @throws IOException if I/O error occurs
     */
    public long getPos() throws IOException {
        return out.getPos();
    }

    private void serializeTsFileMetadata(TsFileMetaData footer) throws IOException {
        long footerIndex = out.getPos();
        LOG.debug("serialize the footer,file pos:{}", footerIndex);
        ReadWriteByteStreamUtils.writeFileMetaData(footer, out);
        LOG.debug("serialize the footer finished, file pos:{}", out.getPos());
        out.write(BytesUtils.longToBytes(footer.getFirstTimeSeriesMetadataOffset()));
        out.write(BytesUtils.longToBytes(footer.getLastTimeSeriesMetadataOffset()));
        out.write(BytesUtils.longToBytes(footer.getFirstTsDeltaObjectMetadataOffset()));
        out.write(BytesUtils.longToBytes(footer.getLastTsDeltaObjectMetadataOffset()));
        out.write(BytesUtils.intToBytes(footer.getCurrentVersion()));
        out.write(magicStringBytes);
    }

    /**
     * fill in output stream to complete row group threshold.
     *
     * @param diff how many bytes that will be filled.
     * @throws IOException if diff is greater than Integer.max_value
     */
    public void fillInRowGroup(long diff) throws IOException {
        if (diff <= Integer.MAX_VALUE) {
            out.write(new byte[(int) diff]);
        } else {
            throw new IOException("write too much blank byte array!array size:" + diff);
        }
    }

    /**
     * Get the list of RowGroupMetaData in memory.
     *
     * @return - current list of RowGroupMetaData
     */
    public List<RowGroupMetaData> getRowGroups() {
        return rowGroupMetaDatas;
    }
}
