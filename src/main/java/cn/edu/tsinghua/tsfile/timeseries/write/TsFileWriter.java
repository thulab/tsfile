package cn.edu.tsinghua.tsfile.timeseries.write;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.file.footer.RowGroupFooter;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementSchema;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.NoMeasurementException;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.JsonConverter;
import cn.edu.tsinghua.tsfile.timeseries.write.series.IChunkGroupWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.series.ChunkGroupWriterImpl;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * TsFileWriter is the entrance for writing processing. It receives a record and send it to
 * responding row group write. It checks memory size for all writing processing along its strategy
 * and flush data stored in memory to OutputStream. At the end of writing, user should call
 * {@code close()} method to flush the last data outside and close the normal outputStream and error
 * outputStream.
 *
 * @author kangrong
 */
public class TsFileWriter {

    private static final Logger LOG = LoggerFactory.getLogger(TsFileWriter.class);

    /**
     * IO writer of this TsFile
     **/
    private final TsFileIOWriter deltaFileWriter;

    /**
     * schema of this TsFile
     **/
    protected final FileSchema schema;
    private final int pageSize;
    private long recordCount = 0;

    /**
     * all IRowGroupWriters
     **/
    private Map<String, IChunkGroupWriter> groupWriters = new HashMap<String, IChunkGroupWriter>();

    /**
     * min value of threshold of data points num check
     **/
    private long recordCountForNextMemCheck = 100;
    private long rowGroupSizeThreshold;

    /**
     * init this TsFileWriter
     *
     * @param file the File to be written by this TsFileWriter
     * @throws IOException
     */
    public TsFileWriter(File file) throws IOException {
        this(new TsFileIOWriter(file), new FileSchema(), TSFileDescriptor.getInstance().getConfig());
    }

    /**
     * init this TsFileWriter
     *
     * @param file   the File to be written by this TsFileWriter
     * @param schema the schema of this TsFile
     * @throws IOException
     */
    public TsFileWriter(File file, FileSchema schema) throws IOException {
        this(new TsFileIOWriter(file), schema, TSFileDescriptor.getInstance().getConfig());
    }

    /**
     * init this TsFileWriter
     *
     * @param file the File to be written by this TsFileWriter
     * @param conf the configuration of this TsFile
     * @throws IOException
     */
    public TsFileWriter(File file, TSFileConfig conf) throws IOException {
        this(new TsFileIOWriter(file), new FileSchema(), conf);
    }

    /**
     * init this TsFileWriter
     *
     * @param file   the File to be written by this TsFileWriter
     * @param schema the schema of this TsFile
     * @param conf   the configuration of this TsFile
     * @throws IOException
     */
    public TsFileWriter(File file, FileSchema schema, TSFileConfig conf)
            throws IOException {
        this(new TsFileIOWriter(file), schema, conf);
    }

    /**
     * init this TsFileWriter
     *
     * @param tsfileWriter the io writer of this TsFile
     * @param schema       the schema of this TsFile
     * @param conf         the configuration of this TsFile
     */
    protected TsFileWriter(TsFileIOWriter tsfileWriter, FileSchema schema, TSFileConfig conf) {
        this.deltaFileWriter = tsfileWriter;
        this.schema = schema;
        this.pageSize = conf.pageSizeInByte;
        this.rowGroupSizeThreshold = conf.groupSizeInByte;
    }

    /**
     * add a MeasurementSchema to this TsFile
     */
    public void addMeasurement(MeasurementSchema measurementSchema)
            throws WriteProcessException {
        if (schema.hasMeasurement(measurementSchema.getMeasurementId()))
            throw new WriteProcessException(
                    "given measurement has exists! " + measurementSchema.getMeasurementId());
        schema.registerMeasurement(measurementSchema);
    }

    /**
     * add a new measurement according to json string.
     *
     * @param measurement example:
     *                    {
     *                    "measurement_id": "sensor_cpu_50",
     *                    "data_type": "INT32",
     *                    "encoding": "RLE"
     *                    "compressor": "SNAPPY"
     *                    }
     * @throws WriteProcessException if the json is illegal or the measurement exists
     */
    public void addMeasurementByJson(JSONObject measurement) throws WriteProcessException {
        addMeasurement(JsonConverter.convertJsonToMeasureMentDescriptor(measurement));
    }


    /**
     * Confirm whether the record is legal. If legal, add it into this RecordWriter.
     *
     * @param record - a record responding a line
     * @return - whether the record has been added into RecordWriter legally
     * @throws WriteProcessException exception
     */
    protected boolean checkIsTimeSeriesExist(TSRecord record) throws WriteProcessException {
        IChunkGroupWriter groupWriter;
        if (!groupWriters.containsKey(record.deviceId)) {
            groupWriter = new ChunkGroupWriterImpl(record.deviceId);
            groupWriters.put(record.deviceId, groupWriter);
        } else {
            groupWriter = groupWriters.get(record.deviceId);
        }

        // add all SeriesWriter of measurements in this TSRecord to this RowGroupWriter
        Map<String, MeasurementSchema> schemaDescriptorMap = schema.getAllMeasurementSchema();
        for (DataPoint dp : record.dataPointList) {
            String measurementId = dp.getMeasurementId();
            if (schemaDescriptorMap.containsKey(measurementId))
                groupWriter.addSeriesWriter(schemaDescriptorMap.get(measurementId), pageSize);
            else
                throw new NoMeasurementException("input measurement is invalid: " + measurementId);
        }
        return true;
    }

    /**
     * write a record in type of T.
     *
     * @param record - record responding a data line
     * @return true -size of tsfile or metadata reaches the threshold.
     * false - otherwise
     * @throws IOException           exception in IO
     * @throws WriteProcessException exception in write process
     */
    public boolean write(TSRecord record) throws IOException, WriteProcessException {

        // make sure the RowGroupWriter for this TSRecord exist
        if (checkIsTimeSeriesExist(record)) {

            // get corresponding RowGroupWriter and write this TSRecord
            groupWriters.get(record.deviceId).write(record.time, record.dataPointList);
            ++recordCount;
            return checkMemorySizeAndMayFlushGroup();
        }
        return false;
    }

    /**
     * calculate total memory size occupied by all RowGroupWriter instances currently.
     *
     * @return total memory size used
     */
    public long calculateMemSizeForAllGroup() {
        int memTotalSize = 0;
        for (IChunkGroupWriter group : groupWriters.values()) {
            memTotalSize += group.updateMaxGroupMemSize();
        }
        return memTotalSize;
    }


    /**
     * check occupied memory size, if it exceeds the rowGroupSize threshold, flush them to given
     * OutputStream.
     *
     * @return true - size of tsfile or metadata reaches the threshold.
     * false - otherwise
     * @throws IOException exception in IO
     */
    private boolean checkMemorySizeAndMayFlushGroup() throws IOException {
        if (recordCount >= recordCountForNextMemCheck) {
            long memSize = calculateMemSizeForAllGroup();
            if (memSize > rowGroupSizeThreshold) {
                LOG.info("start_flush_row_group, memory space occupy:" + memSize);
                recordCountForNextMemCheck = recordCount * rowGroupSizeThreshold / memSize;
                LOG.debug("current threshold:{}, next check:{}", recordCount, recordCountForNextMemCheck);
                return flushAllRowGroups();
            } else {
                recordCountForNextMemCheck = recordCount * rowGroupSizeThreshold / memSize;
                LOG.debug("current threshold:{}, next check:{}", recordCount, recordCountForNextMemCheck);
                return false;
            }
        }

        return false;
    }


    /**
     * flush the data in all series writers of all rowgroup writers and their page writers to outputStream.
     *
     * @return true - size of tsfile or metadata reaches the threshold.
     * false - otherwise. But this function just return false, the Override of IoTDB may return true.
     * @throws IOException exception in IO
     */
    protected boolean flushAllRowGroups() throws IOException {
        if (recordCount > 0) {
            long totalMemStart = deltaFileWriter.getPos();
            //make sure all the pages have been compressed into buffers, so that we can get correct groupWriter.getCurrentRowGroupSize().
            for (IChunkGroupWriter writer : groupWriters.values()) {
                writer.preFlush();
            }
            for (String deviceId : groupWriters.keySet()) {
                long memSize = deltaFileWriter.getPos();
                IChunkGroupWriter groupWriter = groupWriters.get(deviceId);
                long RowGroupSize = groupWriter.getCurrentRowGroupSize();
                RowGroupFooter rowGroupFooter = deltaFileWriter.startFlushRowGroup(deviceId, RowGroupSize, groupWriter.getSeriesNumber());
                groupWriter.flushToFileWriter(deltaFileWriter);

                if (deltaFileWriter.getPos() - memSize != RowGroupSize)
                    throw new IOException(String.format("Flushed data size is inconsistent with computation! Estimated: %d, Actuall: %d",
                            RowGroupSize, deltaFileWriter.getPos() - memSize));

                deltaFileWriter.endRowGroup(deltaFileWriter.getPos() - memSize, rowGroupFooter);
            }
            long actualTotalRowGroupSize = deltaFileWriter.getPos() - totalMemStart;
            LOG.info("total row group size:{}", actualTotalRowGroupSize);
            LOG.info("write row group end");
            recordCount = 0;
            reset();
        }
        return false;
    }


    private void reset() {
        groupWriters.clear();
    }

    /**
     * calling this method to write the last data remaining in memory and close the normal and error
     * OutputStream.
     *
     * @throws IOException exception in IO
     */
    public void close() throws IOException {
        LOG.info("start close file");
        flushAllRowGroups();
        deltaFileWriter.endFile(this.schema);
    }
}
