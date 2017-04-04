package cn.edu.thu.tsfile.timeseries.utils.freq;

import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;

import java.util.List;

public class FrequencyUtil {
    // TODO Check whether these MainFrequencies are satisfied for given freqency filter
    public static boolean satisfy(SingleSeriesFilterExpression freqFilter, List<float[]> freqs) {
        if (freqFilter != null) {
            SingleValueVisitor<Float> visitor = new SingleValueVisitor<>();
            double count = freqs.stream().filter((f) -> f.length >= 2 && visitor.satisfy(f[1], freqFilter)).count();
            if(count/freqs.size() > TSFileDescriptor.getInstance().getConfig().dftSatisfyRate)
                return true;
        }
        return false;
    }
}
