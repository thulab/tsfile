package com.corp.delta.tsfile.write.io;

import com.corp.delta.tsfile.common.conf.TSFileDescriptor;
import com.corp.delta.tsfile.common.utils.BytesUtils;
import com.corp.delta.tsfile.common.utils.TSRandomAccessFileWriter;
import com.corp.delta.tsfile.common.utils.bytesinput.BytesInput;
import com.corp.delta.tsfile.file.metadata.RowGroupMetaData;
import com.corp.delta.tsfile.file.metadata.TInTimeSeriesChunkMetaData;
import com.corp.delta.tsfile.file.metadata.TSDigest;
import com.corp.delta.tsfile.file.metadata.TSFileMetaData;
import com.corp.delta.tsfile.file.metadata.TimeSeriesChunkMetaData;
import com.corp.delta.tsfile.file.metadata.TimeSeriesMetadata;
import com.corp.delta.tsfile.file.metadata.VInTimeSeriesChunkMetaData;
import com.corp.delta.tsfile.file.metadata.converter.TSFileMetaDataConverter;
import com.corp.delta.tsfile.file.metadata.enums.CompressionTypeName;
import com.corp.delta.tsfile.file.metadata.enums.TSChunkType;
import com.corp.delta.tsfile.file.metadata.enums.TSDataType;
import com.corp.delta.tsfile.file.metadata.statistics.Statistics;
import com.corp.delta.tsfile.file.utils.ReadWriteThriftFormatUtils;
import com.corp.delta.tsfile.write.desc.MeasurementDescriptor;
import com.corp.delta.tsfile.write.schema.FileSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * TSFileIOWriter is used to construct metadata and write data stored in memory to output stream.
 * 
 * @author kangrong
 *
 */
public class TSFileIOWriter {
    public static final String MAGIC_STRING = "TsFilev0.0.1";
    public static final byte[] magicStringBytes;
    private static final Logger LOG = LoggerFactory.getLogger(TSFileIOWriter.class);
    public static final TSFileMetaDataConverter metadataConverter = new TSFileMetaDataConverter();
    private final FileSchema schema;
    private final TSRandomAccessFileWriter out;
    private RowGroupMetaData currentRowGroup;
    private TimeSeriesChunkMetaData currentSeries;
    protected List<RowGroupMetaData> rowGroups = new ArrayList<>();

    static {
        magicStringBytes = BytesUtils.StringToBytes(MAGIC_STRING);
    }

    public TSFileIOWriter(FileSchema schema, TSRandomAccessFileWriter output) throws IOException {
        this.schema = schema;
        this.out = output;
        startFile();
    }

    /**
     * write given BytesInput to output stream. This method is called when total memory size exceeds
     * the row group size threshold.
     * 
     * @param bytes - data in page writer
     * @throws IOException
     */
    public void writeBytesToStream(BytesInput bytes) throws IOException {
        bytes.writeAllTo(out.getOutputStream());
    }

    private void startFile() throws IOException {
        out.write(magicStringBytes);
    }

    /**
     * start a {@linkplaincom.corp.delta.tsfile.file.metadata.RowGroupMetaData.RowGroupMetaData
     * RowGroupMetaData}
     * 
     * @param recordCount - the record count of this time series inputed in this stage
     * @param deltaObjectId - delta object id
     * @param deltaObjectType - delta type of this row group
     * @throws IOException
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
     * {@linkplain com.corp.delta.tsfile.file.metadata.TimeSeriesChunkMetaData
     * TimeSeriesChunkMetaData}
     * 
     * @param descriptor - measurement of this time series
     * @param compressionCodecName - compression name of this time series
     * @param tsDataType - data type
     * @param statistics - statistic of the whole series
     * @param maxTime - maximum timestamp of the whole series in this stage
     * @param minTime - minimum timestamp of the whole series in this stage
     * @throws IOException
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
     * write {@linkplain com.corp.delta.tsfile.file.metadata.TSFileMetaData TSFileMetaData} to
     * output stream and close it.
     * 
     * @throws IOException
     */
    public void endFile() throws IOException {
        List<TimeSeriesMetadata> timeSeriesList = schema.getTimeSeriesMetadatas();
        LOG.debug("get time series list:{}", timeSeriesList);
        TSFileMetaData tsfileMetadata =
                new TSFileMetaData(rowGroups, timeSeriesList, TSFileDescriptor.getInstance().getConfig().currentVersion);
        serializeTsFileMetadata(tsfileMetadata);
        out.close();
        LOG.info("output stream is closed");
    }

    /**
     * get the length of normal OutputStream
     * 
     * @return - length of normal OutputStream
     * @throws IOException
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
        if(diff <= Integer.MAX_VALUE)
            out.write(new byte[(int)diff]);
        else
            throw new IOException("write too much blank byte array!array size:"+diff);
    }
}
