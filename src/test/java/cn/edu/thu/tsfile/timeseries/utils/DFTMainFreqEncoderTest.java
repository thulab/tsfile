package cn.edu.thu.tsfile.timeseries.utils;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import cn.edu.thu.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.GtEq;
import cn.edu.thu.tsfile.timeseries.utils.freq.DFTMainFreqEncoder;
import org.junit.Test;

import cn.edu.thu.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.GtEq;
import cn.edu.thu.tsfile.timeseries.utils.freq.DFTMainFreqEncoder;

public class DFTMainFreqEncoderTest {


    @Test
    public void test() {
        // pack length = 100
        int packLength = 156;
        DFTMainFreqEncoder encoder = new DFTMainFreqEncoder(packLength);
        encoder.setMainFreqNum(2);
        List<float[]> packs;

        for (int i = 0; i < 1000; i++) {
            // encoder.encode(2*i+3*Math.cos(2*Math.PI*0.4*i));
//            encoder.encode(3 * Math.cos(2 * Math.PI * 0.4 * i));
             encoder.encode(12.5);
        }
        for (int i = 2001; i < 3000; i++) {
            encoder.encode(37.5 + 10 * Math.cos(2 * Math.PI * 0.4 * i));
            // encoder.encode(i);
        }
        for (int i = 3001; i < 4000; i++) {
            // encoder.encode(2*i+3*Math.cos(2*Math.PI*0.4*i));
//            encoder.encode(3 * Math.cos(2 * Math.PI * 0.4 * i));
             encoder.encode(12.5);
        }

        System.out.println("data: line");
        packs = encoder.getPackFrequency();
        for (int i = 0; i < packs.size(); i++) {
            System.out.println(i * packLength + "\t~\t" + ((i + 1) * packLength - 1) + "\t"
                    + Arrays.toString(packs.get(i)));
        }
        GtEq<Float> gtEq = FilterFactory.gtEq(FilterFactory.floatFilterSeries("a", "b", FilterSeriesType.FREQUENCY_FILTER),0.2f,true);
        assertTrue(encoder.satisfy(gtEq));
        encoder.resetPack();

    }

}
