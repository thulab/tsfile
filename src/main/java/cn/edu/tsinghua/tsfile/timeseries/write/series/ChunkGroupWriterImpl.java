package cn.edu.tsinghua.tsfile.timeseries.write.series;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementSchema;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.NoMeasurementException;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.DataPoint;

/**
 * a implementation of IChunkGroupWriter
 *
 * @author kangrong
 * @see IChunkGroupWriter IChunkGroupWriter
 */
public class ChunkGroupWriterImpl implements IChunkGroupWriter {
    private static Logger LOG = LoggerFactory.getLogger(ChunkGroupWriterImpl.class);
    private final String deviceId;
    /**
     * <measurementID, ChunkWriterImpl>
     */
    private Map<String, IChunkWriter> dataSeriesWriters = new HashMap<String, IChunkWriter>();

    public ChunkGroupWriterImpl(String deviceId) {
        this.deviceId = deviceId;
    }

    @Override
    public void addSeriesWriter(MeasurementSchema schema, int pageSizeThreshold) {
        if (!dataSeriesWriters.containsKey(schema.getMeasurementId())) {
            ChunkBuffer chunkBuffer = new ChunkBuffer(schema);
            IChunkWriter seriesWriter = new ChunkWriterImpl(schema, chunkBuffer, pageSizeThreshold);
            this.dataSeriesWriters.put(schema.getMeasurementId(), seriesWriter);
        }
    }

    @Override
    public void write(long time, List<DataPoint> data) throws WriteProcessException, IOException {
        for (DataPoint point : data) {
            String measurementId = point.getMeasurementId();
            if (!dataSeriesWriters.containsKey(measurementId))
                throw new NoMeasurementException("time " + time + ", measurement id " + measurementId + " not found!");
            point.writeTo(time, dataSeriesWriters.get(measurementId));

        }
    }

    @Override
    public void flushToFileWriter(TsFileIOWriter deltaFileWriter) throws IOException {
        LOG.debug("start flush delta object id:{}", deviceId);
        for (IChunkWriter seriesWriter : dataSeriesWriters.values()) {
            seriesWriter.writeToFileWriter(deltaFileWriter);
        }
    }

    @Override
    public long updateMaxGroupMemSize() {
        long bufferSize = 0;
        for (IChunkWriter seriesWriter : dataSeriesWriters.values())
            bufferSize += seriesWriter.estimateMaxSeriesMemSize();
        return bufferSize;
    }


    @Override
    public long getCurrentRowGroupSize() {
        long size = 0;
        for (IChunkWriter writer : dataSeriesWriters.values()) {
            size += writer.getCurrentChunkSize();
        }
        return size;
    }

    @Override
    public void preFlush() {
        for (IChunkWriter writer : dataSeriesWriters.values()) {
            writer.preFlush();
        }
    }

    @Override
    public int getSeriesNumber() {
        return dataSeriesWriters.size();
    }
}
