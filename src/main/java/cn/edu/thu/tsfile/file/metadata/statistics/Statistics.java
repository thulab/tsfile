package cn.edu.thu.tsfile.file.metadata.statistics;

import java.math.BigDecimal;

import cn.edu.thu.tsfile.common.utils.Binary;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.exception.UnknownColumnTypeException;

/**
 * This class is used for recording statistic information of each measurement in a delta file.While
 * writing processing, the processor records the digest information. Statistics includes maximum,
 * minimum and null value count up to version 0.0.1.<br>
 * Each data type extends this Statistic as super class.<br>
 * 
 * 
 * @since 0.0.1
 * 
 * @author kangrong
 *
 * @param <T>
 */
public abstract class Statistics<T> {
    private static final Logger LOG = LoggerFactory.getLogger(Statistics.class);
    // isEmpty being false means this statistic has been initialized and the max
    // and min is not null;
    protected boolean isEmpty = true;

    abstract public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes);

    abstract public T getMin();

    abstract public T getMax();

    /**
     * static method providing statistic instance for respective data type.
     * 
     * @param type - data type
     * @return
     */
    public static Statistics<?> getStatsByType(TSDataType type) {
        switch (type) {
            case INT32:
                return new IntegerStatistics();
            case INT64:
                return new LongStatistics();
            case BYTE_ARRAY:
                return new BinaryStatistics();
            case ENUMS:
            case BOOLEAN:
                return new NoStatistics();
            case DOUBLE:
                return new DoubleStatistics();
            case FLOAT:
                return new FloatStatistics();
            case BIGDECIMAL:
                return new BigDecimalStatistics();
            default:
                throw new UnknownColumnTypeException(type.toString());
        }
    }

    /**
     * merge parameter to this statistic. Including
     * 
     * @param stats
     * @throws StatisticsClassException
     */
    public void mergeStatistics(Statistics<?> stats) throws StatisticsClassException {
        if (stats == null) {
            LOG.warn("tsfile-file parameter stats is null");
            return;
        }
        if (this.getClass() == stats.getClass()) {
            if (!stats.isEmpty) {
                mergeStatisticsMinMax(stats);
                isEmpty = false;
            }
        } else {
            LOG.warn("tsfile-file Statistics classes mismatched,no merge: "
                    + this.getClass().toString() + " vs. " + stats.getClass().toString());

            throw new StatisticsClassException(this.getClass(), stats.getClass());
        }
    }

    abstract protected void mergeStatisticsMinMax(Statistics<?> stats);

    public boolean isEmpty() {
        return isEmpty;
    }

    public void updateStats(boolean value) {
        throw new UnsupportedOperationException();
    }

    public void updateStats(int value) {
        throw new UnsupportedOperationException();
    }

    public void updateStats(long value) {
        throw new UnsupportedOperationException();
    }

    /**
     * This method with two parameters is only used by {@code overflow} which
     * updates/inserts/deletes timestamp.
     * 
     * @param min
     * @param max
     */
    public void updateStats(long min, long max) {
        throw new UnsupportedOperationException();
    }

    public void updateStats(float value) {
        throw new UnsupportedOperationException();
    }

    public void updateStats(double value) {
        throw new UnsupportedOperationException();
    }

    public void updateStats(BigDecimal value) {
        throw new UnsupportedOperationException();
    }

    public void updateStats(Binary value) {
        throw new UnsupportedOperationException();
    }

    public void reset() {}

    abstract public byte[] getMaxBytes();

    abstract public byte[] getMinBytes();
}
