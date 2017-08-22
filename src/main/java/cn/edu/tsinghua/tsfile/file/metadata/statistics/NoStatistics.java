package cn.edu.tsinghua.tsfile.file.metadata.statistics;


import cn.edu.tsinghua.tsfile.common.utils.Binary;

/**
 * This statistic is used as Unsupported data type. It just return a 0-byte array while asked max or
 * min.
 *
 * @author kangrong
 */
public class NoStatistics extends Statistics<Long> {
    @Override
    public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    }

    @Override
    public Long getMin() {
        return null;
    }

    @Override
    public Long getMax() {
        return null;
    }

    @Override
    public void updateStats(boolean value) {
    }

    @Override
    public void updateStats(int value) {
    }

    @Override
    public void updateStats(long value) {
    }

    @Override
    public void updateStats(Binary value) {
    }

    @Override
    protected void mergeStatisticsMinMax(Statistics<?> stats) {
    }

    @Override
    public byte[] getMaxBytes() {
        return new byte[0];
    }

    @Override
    public byte[] getMinBytes() {
        return new byte[0];
    }

    @Override
    public String toString() {
        return "no stats";
    }
}
