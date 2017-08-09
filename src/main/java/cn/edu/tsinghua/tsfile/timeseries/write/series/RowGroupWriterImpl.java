package cn.edu.tsinghua.tsfile.timeseries.write.series;

import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.NoMeasurementException;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TSFileIOWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.page.IPageWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.page.PageWriterImpl;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * a implementation of IRowGroupWriter
 *
 * @author kangrong
 * @see IRowGroupWriter IRowGroupWriter
 */
public class RowGroupWriterImpl implements IRowGroupWriter {
    private static Logger LOG = LoggerFactory.getLogger(RowGroupWriterImpl.class);
    private final String deltaObjectId;
    private Map<String, ISeriesWriter> dataSeriesWriters = new HashMap<String, ISeriesWriter>();

    public RowGroupWriterImpl(String deltaObjectId, FileSchema fileSchema, int pageSizeThreshold) {
        this.deltaObjectId = deltaObjectId;
        for (MeasurementDescriptor desc : fileSchema.getDescriptor()) {
            this.dataSeriesWriters.put(desc.getMeasurementId(), createSeriesWriter(desc, pageSizeThreshold));
        }
    }

    private ISeriesWriter createSeriesWriter(MeasurementDescriptor desc, int pageSizeThreshold) {
        IPageWriter pageWriter = new PageWriterImpl(desc);
        return new SeriesWriterImpl(deltaObjectId, desc, pageWriter, pageSizeThreshold);
    }

    @Override
    public void write(long time, List<DataPoint> data) throws WriteProcessException, IOException {
        for (DataPoint point : data) {
            String measurementId = point.getMeasurementId();
            if (!dataSeriesWriters.containsKey(measurementId))
                throw new NoMeasurementException("time " + time + ", measurement id " + measurementId + " not found!");
            point.write(time, dataSeriesWriters.get(measurementId));

        }
    }

    @Override
    public List<Object> query(String measurementId) {
        if (dataSeriesWriters.get(measurementId) == null) {
            LOG.warn("The measurementId {} is not exist", measurementId);
            DynamicOneColumnData left = null;
            Pair<List<ByteArrayInputStream>, CompressionTypeName> right = null;
            List<Object> result = new ArrayList<>();
            result.add(left);
            result.add(right);
            return result;
        }
        return dataSeriesWriters.get(measurementId).query();
    }

    @Override
    public void flushToFileWriter(TSFileIOWriter deltaFileWriter) throws IOException {
        LOG.debug("start flush delta object id:{}", deltaObjectId);
        for (ISeriesWriter seriesWriter : dataSeriesWriters.values()) {
            seriesWriter.writeToFileWriter(deltaFileWriter);
        }
    }

    @Override
    public long updateMaxGroupMemSize() {
        long bufferSize = 0;
        for (ISeriesWriter seriesWriter : dataSeriesWriters.values())
            bufferSize += seriesWriter.estimateMaxSeriesMemSize();
        return bufferSize;
    }
}
