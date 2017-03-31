package com.corp.delta.tsfile.utils.freq;

import com.corp.delta.tsfile.common.conf.TSFileDescriptor;
import com.corp.delta.tsfile.filter.definition.SingleSeriesFilterExpression;
import com.corp.delta.tsfile.filter.visitorImpl.SingleValueVisitor;

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
