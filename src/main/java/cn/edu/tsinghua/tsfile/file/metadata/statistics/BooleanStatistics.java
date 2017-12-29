package cn.edu.tsinghua.tsfile.file.metadata.statistics;

import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;

/**
 * @author CGF
 */
public class BooleanStatistics extends Statistics<Boolean> {
	private boolean max;
	private boolean min;
	private boolean first;
	private double sum;

	@Override
	public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
		max = BytesUtils.bytesToBool(maxBytes);
		min = BytesUtils.bytesToBool(minBytes);
	}

	@Override
	public void updateStats(boolean value) {
		if (isEmpty) {
			initializeStats(value, value, value, 0);
			isEmpty = false;
		} else {
			updateStats(value, value, value, 0);
			isEmpty = false;
		}
	}

	private void updateStats(boolean minValue, boolean maxValue, boolean firstValue, double sumValue) {
		if (!minValue && min) {
			min = minValue;
		}
		if (maxValue && !max) {
			max = maxValue;
		}
	}

	@Override
	public Boolean getMax() {
		return max;
	}

	@Override
	public Boolean getMin() {
		return min;
	}

	@Override
	public Boolean getFirst() {
		return first;
	}

	@Override
	public double getSum() {
		return sum;
	}

	@Override
	protected void mergeStatisticsValue(Statistics<?> stats) {
		BooleanStatistics boolStats = (BooleanStatistics) stats;
		if (isEmpty) {
			initializeStats(boolStats.getMin(), boolStats.getMax(), boolStats.getFirst(), boolStats.getSum());
			isEmpty = false;
		} else {
			updateStats(boolStats.getMin(), boolStats.getMax(), boolStats.getFirst(), boolStats.getSum());
		}

	}

	public void initializeStats(boolean min, boolean max, boolean firstValue, double sumValue) {
		this.min = min;
		this.max = max;
		this.first = firstValue;
	}

	@Override
	public byte[] getMaxBytes() {
		return BytesUtils.boolToBytes(max);
	}

	@Override
	public byte[] getMinBytes() {
		return BytesUtils.boolToBytes(min);
	}

	@Override
	public byte[] getFirstBytes() {
		return BytesUtils.boolToBytes(first);
	}

	@Override
	public byte[] getSumBytes() {
		return BytesUtils.doubleToBytes(sum);
	}

	@Override
	public String toString() {
		return "[max:" + max + ",min:" + min + ",first:" + first + ",sum:" + sum + "]";
	}
}
