package com.corp.delta.tsfile.write.series;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.corp.delta.tsfile.write.desc.MeasurementDescriptor;
import com.corp.delta.tsfile.write.exception.NoMeasurementException;
import com.corp.delta.tsfile.write.exception.WriteProcessException;
import com.corp.delta.tsfile.write.io.TSFileIOWriter;
import com.corp.delta.tsfile.write.page.IPageWriter;
import com.corp.delta.tsfile.write.page.PageWriterImpl;
import com.corp.delta.tsfile.write.record.DataPoint;
import com.corp.delta.tsfile.write.schema.FileSchema;

/**
 * a implementation of IRowGroupWriter
 * 
 * @see com.corp.delta.tsfile.write.series.IRowGroupWriter IRowGroupWriter
 * @author kangrong
 *
 */
public class RowGroupWriterImpl implements IRowGroupWriter {
    private static Logger LOG = LoggerFactory.getLogger(RowGroupWriterImpl.class);
    private final String deltaObjectId;
    private Map<String, ISeriesWriter> dataSeriesWriters = new HashMap<String, ISeriesWriter>();

    public RowGroupWriterImpl(String deltaObjectId, FileSchema fileSchema, int pageSizeThreshold) {
        this.deltaObjectId = deltaObjectId;
        for (MeasurementDescriptor desc : fileSchema.getDescriptor()) {
            this.dataSeriesWriters.put(desc.getMeasurementId(),
                    createSeriesWriter(desc, pageSizeThreshold));
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
                throw new NoMeasurementException("time " + time + ", measurement id "
                        + measurementId + " not found!");
            point.write(time, dataSeriesWriters.get(measurementId));

        }
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
