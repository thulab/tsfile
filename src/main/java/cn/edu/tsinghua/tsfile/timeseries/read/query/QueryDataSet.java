package cn.edu.tsinghua.tsfile.timeseries.read.query;

import cn.edu.tsinghua.tsfile.timeseries.read.datatype.RowRecord;

import java.io.IOException;

/**
 * Created by zhangjinrui on 2017/12/13.
 */
public interface QueryDataSet {

    /**
     * check if unread data still exists
     * @return whether the QueryDataSet has the next data
     * @throws IOException IOException
     */
    boolean hasNext() throws IOException;

    /**
     * get the next unread data
     * another data will be returned when calling this method next time
     * @return the next RowRecord
     * @throws IOException IOException
     */
    RowRecord next() throws IOException;

}
