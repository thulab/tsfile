package cn.edu.thu.tsfile.timeseries.filter.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.exception.FilterInvokeException;

/**
 * used for filter Float type optimization see {@link Interval}
 * 
 * @author CGF
 *
 */
public class FloatInterval extends Interval {
	private static final Logger LOG = LoggerFactory.getLogger(FloatInterval.class);
	
	public float[] v = new float[arrayMaxn];
	
	public void addValueFlag(float value, boolean f) {
		if(count >= arrayMaxn - 2) {
			LOG.error("IntInterval array length spill.");
			throw new FilterInvokeException("FloatInterval array length spill.");
		}
		v[count] = value;
		flag[count] = f;
		count++;
	}
	
	public String toString() {
		StringBuffer ans = new StringBuffer();
		for (int i = 0; i < count; i += 2) {
			if (flag[i])
				ans.append("[" + v[i] + ",");
			else
				ans.append("(" + v[i] + ",");
			if (flag[i + 1])
				ans.append(v[i + 1] + "]");
			else
				ans.append(v[i + 1] + ")");
		}
		return ans.toString();
	}
}
