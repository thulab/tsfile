package cn.edu.tsinghua.tsfile.timeseries.write.series;

import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TSFileIOWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;

import java.io.IOException;
import java.util.List;

/**
 * a row group in TSFile contains a list of value series. TimeSeriesGroupWriter
 * should implement write method which inputs a time stamp(in TimeValue class)
 * and a list of data points. It also should provide flushing method for
 * outputting to OS file system or HDFS.
 *
 * @author kangrong
 */
public interface IRowGroupWriter {
    /**
     * receive a timestamp and a list of data points, write them to themselves
     * series writers.
     *
     * @param time - all data points have unify time stamp.
     * @param data - data point list to input
     * @throws WriteProcessException
     * @throws IOException
     */
    void write(long time, List<DataPoint> data) throws WriteProcessException, IOException;

    /**
     * flushing method for outputting to OS file system or HDFS.
     *
     * @param tsfileWriter - TSFileIOWriter
     * @throws IOException
     */
    void flushToFileWriter(TSFileIOWriter tsfileWriter) throws IOException;

    /**
     * query this measurementId data in memory
     *
     * @param measurementId
     * @return fist object is the current page data, second object is the all pages which is packaged
     */
    List<Object> query(String measurementId);

    /**
     * Note that, this method should be called after running
     * {@code long calcAllocatedSize()}
     *
     * @return - allocated memory size.
     */
    long updateMaxGroupMemSize();
}
