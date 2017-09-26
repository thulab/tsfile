package cn.edu.tsinghua.tsfile.timeseries.write;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.NoMeasurementException;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TSFileIOWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.converter.JsonConverter;
import cn.edu.tsinghua.tsfile.timeseries.write.series.IRowGroupWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.series.RowGroupWriterImpl;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code InternalRecordWriter<T>} is the entrance for writing processing. It
 * receives a record in type of {@code T} and send it to responding row group
 * write. It checks memory size for all writing processing along its strategy
 * and flush data stored in memory to OutputStream. At the end of writing, user
 * should call {@code close()} method to flush the last data outside and close
 * the normal outputStream and error outputStream.
 *
 * @param <T> - record type
 * @author kangrong
 */
public abstract class InternalRecordWriter<T> {
    private static final Logger LOG = LoggerFactory.getLogger(InternalRecordWriter.class);
    private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 1;
    protected final TSFileIOWriter deltaFileWriter;
    protected final WriteSupport<T> writeSupport;
    protected final FileSchema schema;
    protected final int pageSize;
    protected final long primaryRowGroupSize;
    protected long recordCount = 0;
    protected Map<String, IRowGroupWriter> groupWriters = new HashMap<String, IRowGroupWriter>();
    private long recordCountForNextMemCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;
    private long rowGroupSizeThreshold;
    private int oneRowMaxSize;

    public InternalRecordWriter(TSFileConfig conf, TSFileIOWriter tsfileWriter, WriteSupport<T> writeSupport,
                                FileSchema schema) throws WriteProcessException {
        this.deltaFileWriter = tsfileWriter;
        this.writeSupport = writeSupport;
        this.schema = schema;
        this.primaryRowGroupSize = conf.groupSizeInByte;
        this.pageSize = conf.pageSizeInByte;
        this.oneRowMaxSize = schema.getCurrentRowMaxSize();
        if(primaryRowGroupSize <= oneRowMaxSize)
            throw new WriteProcessException("initial measurement error: the potential size of one row is too large");
        this.rowGroupSizeThreshold = primaryRowGroupSize - oneRowMaxSize;
        writeSupport.init(groupWriters);
    }

    public void addMeasurementByJson(JSONObject measurement) throws WriteProcessException {
        JsonConverter.addJsonToMeasurement(measurement, schema);
        this.oneRowMaxSize = schema.getCurrentRowMaxSize();
        if(primaryRowGroupSize <= oneRowMaxSize)
            throw new WriteProcessException("add measurement error: the potential size of one row is too large");
        this.rowGroupSizeThreshold = primaryRowGroupSize - oneRowMaxSize;
        try {
            checkMemorySize();
        } catch (IOException e) {
            throw new WriteProcessException(e.getMessage());
        }
    }

    /**
     * Confirm whether the record is legal. If legal, add it into this
     * RecordWriter.
     *
     * @param record - a record responding a line
     * @return - whether the record has been added into RecordWriter legally
     * @throws IOException exception in IO
     */
    abstract protected boolean checkRowGroup(T record) throws IOException, WriteProcessException;

    /**
     * write a record in type of T
     * @param record - record responding a data line
     * @throws IOException exception in IO
     * @throws WriteProcessException exception in write process
     */
    public void write(T record) throws IOException, WriteProcessException {
        if (checkRowGroup(record)) {
            writeSupport.write(record);
            ++recordCount;
            checkMemorySize();
        }

    }

    public List<Object> query(String deltaObjectId, String measurementId) {

        return writeSupport.query(deltaObjectId, measurementId);
    }

    /**
     * <b>Note that</b>, before calling this method, all {@code IRowGroupWriter}
     * instance existing in {@code groupWriters} have been reset for next
     * writing stage, thus we don't add new {@code IRowGroupWriter} if its
     * deltaObjecyId has existed.
     *
     * @param record - delta object to be add
     */
    protected void addGroupToInternalRecordWriter(TSRecord record) throws WriteProcessException {
        IRowGroupWriter groupWriter;
        if (!groupWriters.containsKey(record.deltaObjectId)) {
            groupWriter = new RowGroupWriterImpl(record.deltaObjectId);
            groupWriters.put(record.deltaObjectId, groupWriter);
        } else
            groupWriter = groupWriters.get(record.deltaObjectId);
        Map<String, MeasurementDescriptor> schemaDescriptorMap = schema.getDescriptor();
        for (DataPoint dp : record.dataPointList) {
            String measurementId = dp.getMeasurementId();
            if (schemaDescriptorMap.containsKey(measurementId))
                groupWriter.addSeriesWriter(schemaDescriptorMap.get(measurementId), pageSize);
            else
                throw new NoMeasurementException("input measurement is invalid: " + measurementId);
        }
    }

    /**
     * calculate total memory size occupied by all RowGroupWriter instances
     *
     * @return total memory size used
     */
    public long updateMemSizeForAllGroup() {
        int memTotalSize = 0;
        for (IRowGroupWriter group : groupWriters.values()) {
            memTotalSize += group.updateMaxGroupMemSize();
        }
        return memTotalSize;
    }

    /**
     * check occupied memory size, if it exceeds the rowGroupSize threshold,
     * flush them to given OutputStream.
     *
     * @throws IOException exception in IO
     */
    protected void checkMemorySize() throws IOException {
        if (recordCount >= recordCountForNextMemCheck) {
            long memSize = updateMemSizeForAllGroup();
            if (memSize > rowGroupSizeThreshold) {
                LOG.info("start_write_row_group, memory space occupy:" + memSize);
                flushRowGroup(true);
                recordCountForNextMemCheck = rowGroupSizeThreshold / oneRowMaxSize;
            } else {
                recordCountForNextMemCheck = recordCount + (rowGroupSizeThreshold - memSize) / oneRowMaxSize;
            }
        }
    }

    /**
     * flush the data in all series writers and their page writers to
     * outputStream.
     * @param isFillRowGroup whether to fill RowGroup
     * @throws IOException exception in IO
     */
    protected void flushRowGroup(boolean isFillRowGroup) throws IOException {
        // at the present stage, just flush one block
        String deltaType = schema.getDeltaType();
        if (recordCount > 0) {
            long totalMemStart = deltaFileWriter.getPos();
            for (String deltaObjectId : schema.getDeltaObjectAppearedSet()) {
                long memSize = deltaFileWriter.getPos();
                deltaFileWriter.startRowGroup(recordCount, deltaObjectId, deltaType);
                IRowGroupWriter groupWriter = groupWriters.get(deltaObjectId);
                groupWriter.flushToFileWriter(deltaFileWriter);
                deltaFileWriter.endRowGroup(deltaFileWriter.getPos() - memSize);
            }
            long actualTotalRowGroupSize = deltaFileWriter.getPos() - totalMemStart;
            if (isFillRowGroup) {
                fillInRowGroupSize(actualTotalRowGroupSize);
                LOG.info("total row group size:{}, actual:{}, filled:{}", primaryRowGroupSize, actualTotalRowGroupSize,
                        primaryRowGroupSize - actualTotalRowGroupSize);
            } else
                LOG.info("total row group size:{}, row group is not filled", actualTotalRowGroupSize);
            LOG.info("write row group end");
            recordCount = 0;
            reset();
        }
    }

    protected void fillInRowGroupSize(long actualRowGroupSize) throws IOException {
        if (actualRowGroupSize > primaryRowGroupSize)
            LOG.warn("too large actual row group size!:actual:{},threshold:{}", actualRowGroupSize,
                    primaryRowGroupSize);
        deltaFileWriter.fillInRowGroup(primaryRowGroupSize - actualRowGroupSize);
    }

    /**
     * <b>Note that</b> we don't need to reset RowGroupWriter explicitly, since
     * after calling {@code flushToFileWriter()}, RowGroupWriter resets itself.
     */
    private void reset() {
        schema.resetUnusedDeltaObjectId(groupWriters);
    }

    /**
     * calling this method to write the last data remaining in memory and close
     * the normal and error OutputStream
     *
     * @throws IOException exception in IO
     */
    public void close() throws IOException {
        LOG.info("start close file");
        updateMemSizeForAllGroup();
        flushRowGroup(false);
        deltaFileWriter.endFile();
    }
}
