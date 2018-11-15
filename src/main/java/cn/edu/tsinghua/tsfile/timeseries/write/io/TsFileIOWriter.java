package cn.edu.tsinghua.tsfile.timeseries.write.io;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.constant.StatisticConstant;
import cn.edu.tsinghua.tsfile.common.utils.*;
import cn.edu.tsinghua.tsfile.file.header.ChunkHeader;
import cn.edu.tsinghua.tsfile.file.header.RowGroupHeader;
import cn.edu.tsinghua.tsfile.file.metadata.*;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.file.metadata.statistics.Statistics;
import cn.edu.tsinghua.tsfile.common.utils.ReadWriteIOUtils;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * TSFileIOWriter is used to construct metadata and write data stored in memory
 * to output stream.
 *
 * @author kangrong
 */
public class TsFileIOWriter {

    public static final byte[] magicStringBytes;
    private static final Logger LOG = LoggerFactory.getLogger(TsFileIOWriter.class);

    static {
        magicStringBytes = BytesUtils.StringToBytes(TSFileConfig.MAGIC_STRING);
    }

    private ITsRandomAccessFileWriter out;
    protected List<RowGroupMetaData> rowGroupMetaDatas = new ArrayList<>();
    private RowGroupMetaData currentRowGroupMetaData;
    private TimeSeriesChunkMetaData currentChunkMetaData;


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
     * Writes given bytes to output stream.
     * This method is called when total memory size exceeds the row group size
     * threshold.
     *
     * @param bytes - data of several pages which has been packed
     * @throws IOException if an I/O error occurs.
     */
    public void writeBytesToStream(PublicBAOS bytes) throws IOException {
        bytes.writeTo(out.getOutputStream());
    }


    private void startFile() throws IOException {
        out.write(magicStringBytes);
    }

    /**
     * start a {@linkplain RowGroupMetaData RowGroupMetaData}.
     *
     * @param deltaObjectId delta object id
     * @param dataSize      the serialized size of all chunks
     * @return the serialized size of RowGroupHeader
     */
    public int startFlushRowGroup(String deltaObjectId, long dataSize, int numberOfChunks) throws IOException {
        LOG.debug("start row group:{}, file position {}", deltaObjectId, out.getPos());
        currentRowGroupMetaData = new RowGroupMetaData(deltaObjectId, 0, out.getPos(), new ArrayList<>());
        RowGroupHeader header = new RowGroupHeader(deltaObjectId, dataSize, numberOfChunks);
        header.serializeTo(out.getOutputStream());
        LOG.debug("finishing writing row group header {}, file position {}", header, out.getPos());
        return header.getSerializedSize();
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
     * @param datasize             -  the serialized size of all pages
     * @return the serialized size of CHunkHeader
     * @throws IOException if I/O error occurs
     */
    public int startFlushChunk(MeasurementDescriptor descriptor, CompressionType compressionCodecName,
                               TSDataType tsDataType, TSEncoding encodingType, Statistics<?> statistics, long maxTime, long minTime, int datasize, int numOfPages) throws IOException {
        LOG.debug("start series chunk:{}, file position {}", descriptor, out.getPos());

        currentChunkMetaData = new TimeSeriesChunkMetaData(descriptor.getMeasurementId(), out.getPos(), minTime, maxTime);

        ChunkHeader header = new ChunkHeader(descriptor.getMeasurementId(), datasize, tsDataType, compressionCodecName, encodingType, numOfPages);
        header.serializeTo(out.getOutputStream());
        LOG.debug("finish series chunk:{} header, file position {}", header, out.getPos());

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

        return header.getSerializedSize();
    }


    public void endChunk(long totalValueCount) {
        currentChunkMetaData.setNumOfPoints(totalValueCount);
        currentRowGroupMetaData.addTimeSeriesChunkMetaData(currentChunkMetaData);
        LOG.debug("end series chunk:{},totalvalue:{}", currentChunkMetaData, totalValueCount);
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
    	// get all TimeSeriesMetadatas of this TsFile
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

					// update startTime and endTime
                    startTime = Long.min(startTime, timeSeriesChunkMetaData.getStartTime());
                    endTime = Long.max(endTime, timeSeriesChunkMetaData.getEndTime());
                }
            }
            // flush tsRowGroupBlockMetaDatas in order
            currentTsDeltaObjectMetadata.setStartTime(startTime);
            currentTsDeltaObjectMetadata.setEndTime(endTime);
        }

        TsFileMetaData tsFileMetaData = new TsFileMetaData(tsDeltaObjectMetadataMap, timeSeriesList,
                TSFileConfig.currentVersion);

        long footerIndex = out.getPos();
        LOG.debug("start to flush the footer,file pos:{}", footerIndex);
        int size = tsFileMetaData.serializeTo(out.getOutputStream());
        LOG.debug("finish flushing the footer {}, file pos:{}", tsFileMetaData, out.getPos());
        ReadWriteIOUtils.write(size, out.getOutputStream());//write the size of the file metadata.
        out.write(magicStringBytes);
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
        LOG.debug("serializeTo the footer,file pos:{}", footerIndex);

        footer.serializeTo(out.getOutputStream());
        LOG.debug("serializeTo the footer finished, file pos:{}", out.getPos());
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
