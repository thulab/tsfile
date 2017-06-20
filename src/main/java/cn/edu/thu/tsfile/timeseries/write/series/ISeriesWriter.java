package cn.edu.thu.tsfile.timeseries.write.series;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

import cn.edu.thu.tsfile.common.utils.Binary;
import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfile.timeseries.write.io.TSFileIOWriter;

/**
 * ISeriesWriter provides a list of writing methods for different value types.
 * 
 * @author kangrong
 *
 */

public interface ISeriesWriter {
    void write(long time, int value) throws IOException;

    void write(long time, long value) throws IOException;

    void write(long time, boolean value) throws IOException;

    void write(long time, float value) throws IOException;

    void write(long time, double value) throws IOException;

    void write(long time, BigDecimal value) throws IOException;

    void write(long time, Binary value) throws IOException;
    
    Pair<DynamicOneColumnData, Pair<List<ByteArrayInputStream>, CompressionTypeName>> query();

    void writeToFileWriter(TSFileIOWriter tsfileWriter) throws IOException;

    long estimateMaxSeriesMemSize();
}
