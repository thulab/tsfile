package cn.edu.thu.tsfile.timeseries.write;

import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.thu.tsfile.timeseries.write.series.IRowGroupWriter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * {@code WriteSupport<T>} is template class for different input formats. Each
 * input format should implement the method write(T record).
 *
 * @param <T>
 * @author kangrong
 */
abstract public class WriteSupport<T> {
    /**
     * initialize WriteSupport
     *
     * @param groupWriters - a deltaObjectId-IRowGroupWriter map
     */
    public abstract void init(Map<String, IRowGroupWriter> groupWriters);

    /**
     * write method is used to receive record T and arrange it to suitable
     * RowGroupWriter.
     *
     * @param record - input record
     * @throws WriteProcessException
     */
    public abstract void write(T record) throws IOException, WriteProcessException;

    /**
     * query one column data which is in memory.
     *
     * @param deltaObjectId
     * @param measurementId
     * @return first object is {@link DynamicOneColumnData} which is current
     * page data, second object is a list of
     * {@link ByteArrayInputStream} which is all page data packaged
     */
    public abstract List<Object> query(String deltaObjectId, String measurementId);
}
