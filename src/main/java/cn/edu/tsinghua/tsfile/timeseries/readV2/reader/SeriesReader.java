package cn.edu.tsinghua.tsfile.timeseries.readV2.reader;

import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

import java.io.IOException;

/**
 * @author Jinrui Zhang
 */
public interface SeriesReader extends TimeValuePairReader {


    public DynamicOneColumnData getNextBatchData() throws IOException;

}
