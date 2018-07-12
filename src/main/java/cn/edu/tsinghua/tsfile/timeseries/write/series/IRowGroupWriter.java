package cn.edu.tsinghua.tsfile.timeseries.write.series;

import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;

import java.io.IOException;
import java.util.List;

/**
 * a row group in TSFile contains a list of value series. TimeSeriesGroupWriter
 * should implement writeTo method which inputs a time stamp(in TimeValue class)
 * and a list of data points. It also should provide flushing method for
 * outputting to OS file system or HDFS.
 *
 * @author kangrong
 */
public interface IRowGroupWriter {
    /**
     * receive a timestamp and a list of data points, writeTo them to themselves
     * series writers.
     *
     * @param time - all data points have unify time stamp.
     * @param data - data point list to input
     * @throws WriteProcessException exception in writeTo process
     * @throws IOException exception in IO
     */
    void write(long time, List<DataPoint> data) throws WriteProcessException, IOException;

    /**
     * flushing method for outputting to OS file system or HDFS.
     *
     * @param tsfileWriter - TSFileIOWriter
     * @throws IOException exception in IO
     */
    void flushToFileWriter(TsFileIOWriter tsfileWriter) throws IOException;

    /**
     * get the max memory occupied at this time.
     *
     * Note that, this method should be called after running
     * {@code long calcAllocatedSize()}
     *
     * @return - allocated memory size.
     */
    long updateMaxGroupMemSize();

    /**
     * given a measurement descriptor, create a corresponding writer and put into this RowGroupWriter
     *
     * @param measurementDescriptor a measurement descriptor containing the message of the series
     * @param pageSize the specified page size
     */
    void addSeriesWriter(MeasurementDescriptor measurementDescriptor, int pageSize);

    /**
     * @return get the serialized size of current rowGroup header + all chunks
     */
    long getCurrentRowGroupSize();

    /**
     * call all the series to prepare to flush data.
     */
    void preFlush();

    int getSeriesNumber();
}
