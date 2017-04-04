package cn.edu.thu.tsfile.timeseries.utils.freq;

import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfile.encoding.encoder.dft.DFTDoubleEncoder;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DFTMainFreqEncoder extends DFTDoubleEncoder {
    private List<float[]> packFrequency = new ArrayList<float[]>();

    public DFTMainFreqEncoder() {
        super(TSFileDescriptor.getInstance().getConfig().defaultDFTPackLength,
                TSFileDescriptor.getInstance().getConfig().defaultDFTRate,
                TSFileDescriptor.getInstance().getConfig().defaultDFTOverlapRate);
    }

    public DFTMainFreqEncoder(int packLength) {
        super(packLength, TSFileDescriptor.getInstance().getConfig().defaultDFTRate,
                TSFileDescriptor.getInstance().getConfig().defaultDFTOverlapRate);
    }

    public DFTMainFreqEncoder(int packLength, float overlapRate) {
        super(packLength, TSFileDescriptor.getInstance().getConfig().defaultDFTRate, overlapRate);
    }

    public void encode(int value) {
        encode((double) value, null);
    }

    public void encode(long value) {
        encode((double) value, null);
    }

    public void encode(float value) {
        encode((double) value, null);
    }

    public void encode(double value) {
        encode((double) value, null);
    }

    public List<float[]> getPackFrequency() {
        flush(null);
        return packFrequency;
    }

    public boolean satisfy(SingleSeriesFilterExpression freqFilter) {
        flush(null);
        if (freqFilter != null) {
            SingleValueVisitor<Float> visitor = new SingleValueVisitor<>();
            double count = packFrequency.stream().filter((f) -> f.length >= 2 && visitor.satisfy(f[1], freqFilter)).count();
            if(count/packFrequency.size() > TSFileDescriptor.getInstance().getConfig().dftSatisfyRate)
                return true;
        }
        return false;

    }


    @Override
    protected void reset() {
        super.reset();
    }


    public void resetPack() {
        super.reset();
        packFrequency.clear();
    }

    @Override
    protected void writeToOut() throws IOException {
        // write header
        transform();
        float[] mainFreqs = getFFTForwardFrequency(mainFreqNum);
        LOG.info("main frequency:{}", Arrays.toString(mainFreqs));
        packFrequency.add(mainFreqs);

    }

}
