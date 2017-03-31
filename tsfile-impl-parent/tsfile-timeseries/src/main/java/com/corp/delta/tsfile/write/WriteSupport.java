package com.corp.delta.tsfile.write;

import java.io.IOException;
import java.util.Map;

import com.corp.delta.tsfile.write.exception.WriteProcessException;
import com.corp.delta.tsfile.write.series.IRowGroupWriter;

/**
 * {@code WriteSupport<T>} is template class for different input formats. Each input format should
 * implement the method write(T record).
 * 
 * @author kangrong
 *
 * @param <T>
 */
abstract public class WriteSupport<T> {
    /**
     * initialize WriteSupport
     * 
     * @param groupWriters - a deltaObjectId-IRowGroupWriter map
     */
    public abstract void init(Map<String, IRowGroupWriter> groupWriters);

    /**
     * write method is used to receive record T and arrange it to suitable RowGroupWriter.
     * 
     * @param record - input record
     * @throws WriteProcessException
     */
    public abstract void write(T record) throws IOException, WriteProcessException;
}
