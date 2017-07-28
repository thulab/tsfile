package cn.edu.thu.tsfile.timeseries.write.io;

import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfile.common.utils.BytesUtils;
import cn.edu.thu.tsfile.common.utils.ListByteArrayOutputStream;
import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileWriter;
import cn.edu.thu.tsfile.file.metadata.*;
import cn.edu.thu.tsfile.file.metadata.converter.TSFileMetaDataConverter;
import cn.edu.thu.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.thu.tsfile.file.metadata.enums.TSChunkType;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.file.metadata.statistics.Statistics;
import cn.edu.thu.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.thu.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.thu.tsfile.timeseries.write.schema.FileSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * TSFileIOWriter is used to construct metadata and write data stored in memory to output stream.
 *
 * @author kangrong
 */
public class TSFileIOWriter {
    public static final String MAGIC_STRING = "TsFilev0.0.1";
    public static final byte[] magicStringBytes;
    public static final TSFileMetaDataConverter metadataConverter = new TSFileMetaDataConverter();
    private static final Logger LOG = LoggerFactory.getLogger(TSFileIOWriter.class);

    static {
        magicStringBytes = BytesUtils.StringToBytes(MAGIC_STRING);
    }

    private final FileSchema schema;
    private final TSRandomAccessFileWriter out;
    protected List<RowGroupMetaData> rowGroups = new ArrayList<>();
    private RowGroupMetaData currentRowGroup;
    private TimeSeriesChunkMetaData currentSeries;

    /**
     * This is just used to restore one TSFile from List of RowGroupMetaData and the offset
     *
     * @param schema schema containing measurement information
     * @param output be used to output written data
     * @throws IOException if I/O error occurs
     */
    public TSFileIOWriter(FileSchema schema, TSRandomAccessFileWriter output) throws IOException {
        this.schema = schema;
        this.out = output;
        startFile();
    }

    /**
     * This is just used to restore one TSFile from List of RowGroupMetaData and the offset
     *
     * @param schema    schema containing measurement information
     * @param output    be used to output written data
     * @param rowGroups given a constructed row group list for fault recovery
     * @throws IOException if I/O error occurs
     */
    public TSFileIOWriter(FileSchema schema, TSRandomAccessFileWriter output, long offset,
                          List<RowGroupMetaData> rowGroups) throws IOException {
        this.schema = schema;
        this.out = output;
        out.seek(offset);
        this.rowGroups = rowGroups;
    }

    /**
     * Writes given <code>ListByteArrayOutputStream</code> to output stream.
     * This method is called when total memory size exceeds the row group size threshold.
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
     * start a {@linkplain RowGroupMetaData RowGroupMetaData}
     *
     * @param recordCount     - the record count of this time series input in this stage
     * @param deltaObjectId   - delta object id
     * @param deltaObjectType - delta type of this row group
     * @throws IOException if I/O error occurs
     */
    public void startRowGroup(long recordCount, String deltaObjectId, String deltaObjectType)
            throws IOException {
        LOG.debug("start row group:{}", deltaObjectId);
        currentRowGroup =
                new RowGroupMetaData(deltaObjectId, recordCount, 0,
                        new ArrayList<>(), deltaObjectType);
    }

    /**
     * start a
     * {@linkplain cn.edu.thu.tsfile.file.metadata.TimeSeriesChunkMetaData
     * TimeSeriesChunkMetaData}
     *
     * @param descriptor           - measurement of this time series
     * @param compressionCodecName - compression name of this time series
     * @param tsDataType           - data type
     * @param statistics           - statistic of the whole series
     * @param maxTime              - maximum timestamp of the whole series in this stage
     * @param minTime              - minimum timestamp of the whole series in this stage
     * @throws IOException if I/O error occurs
     */
    public void startSeries(MeasurementDescriptor descriptor,
                            CompressionTypeName compressionCodecName, TSDataType tsDataType,
                            Statistics<?> statistics, long maxTime, long minTime) throws IOException {
        LOG.debug("start series:{}", descriptor);
        currentSeries =
                new TimeSeriesChunkMetaData(descriptor.getMeasurementId(), TSChunkType.VALUE,
                        out.getPos(), compressionCodecName);
        TInTimeSeriesChunkMetaData t = new TInTimeSeriesChunkMetaData(tsDataType, minTime, maxTime);
        currentSeries.setTInTimeSeriesChunkMetaData(t);
        byte[] max = statistics.getMaxBytes();
        byte[] min = statistics.getMinBytes();

        VInTimeSeriesChunkMetaData v = new VInTimeSeriesChunkMetaData(tsDataType);
        TSDigest tsDigest =
                new TSDigest(ByteBuffer.wrap(max, 0, max.length), ByteBuffer.wrap(min, 0,
                        min.length));
        v.setDigest(tsDigest);
        descriptor.setDataValues(v);
        currentSeries.setVInTimeSeriesChunkMetaData(v);
    }

    public void endSeries(long size, long totalValueCount) throws IOException {
        LOG.debug("end series:{},totalvalue:{}", currentSeries, totalValueCount);
        currentSeries.setTotalByteSize(size);
        currentSeries.setNumRows(totalValueCount);
        currentRowGroup.addTimeSeriesChunkMetaData(currentSeries);
        currentSeries = null;
    }

    public void endRowGroup(long memSize) throws IOException {
        currentRowGroup.setTotalByteSize(memSize);
        rowGroups.add(currentRowGroup);
        LOG.debug("end row group:{}", currentRowGroup);
        currentRowGroup = null;
    }

    /**
     * write {@linkplain cn.edu.thu.tsfile.file.metadata.TSFileMetaData TSFileMetaData} to
     * output stream and close it.
     *
     * @throws IOException if I/O error occurs
     */
    public void endFile() throws IOException {
        List<TimeSeriesMetadata> timeSeriesList = schema.getTimeSeriesMetadatas();
        LOG.debug("get time series list:{}", timeSeriesList);
        TSFileMetaData tsfileMetadata =
                new TSFileMetaData(rowGroups, timeSeriesList, TSFileDescriptor.getInstance().getConfig().currentVersion);
        Map<String, String> props = schema.getProps();
        tsfileMetadata.setProps(props);
        serializeTsFileMetadata(tsfileMetadata);
        out.close();
        LOG.info("output stream is closed");
    }

    /**
     * get the length of normal OutputStream
     *
     * @return - length of normal OutputStream
     * @throws IOException if I/O error occurs
     */
    public long getPos() throws IOException {
        return out.getPos();
    }

    private void serializeTsFileMetadata(TSFileMetaData footer) throws IOException {
        long footerIndex = out.getPos();
        LOG.debug("serialize the footer,file pos:{}", footerIndex);
        TSFileMetaDataConverter metadataConverter = new TSFileMetaDataConverter();
        ReadWriteThriftFormatUtils.writeFileMetaData(
                metadataConverter.toThriftFileMetadata(footer), out.getOutputStream());
        LOG.debug("serialize the footer finished, file pos:{}", out.getPos());
        out.write(BytesUtils.intToBytes((int) (out.getPos() - footerIndex)));
        out.write(magicStringBytes);
    }

    // fill in output stream to complete row group threshold
    public void fillInRowGroup(long diff) throws IOException {
        if (diff <= Integer.MAX_VALUE)
            out.write(new byte[(int) diff]);
        else
            throw new IOException("write too much blank byte array!array size:" + diff);
    }

    /**
     * Get the list of RowGroupMetaData in memory
     *
     * @return - current list of RowGroupMetaData
     */
    public List<RowGroupMetaData> getRowGroups() {
        return rowGroups;
    }

}
