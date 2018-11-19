package cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator;

import java.io.IOException;

/**
 * Created by zhangjinrui on 2017/12/26.
 */
public interface TimestampGenerator {

    boolean hasNext() throws IOException;

    long next() throws IOException;

}
