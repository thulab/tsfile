package cn.edu.thu.tsfile.encoding.encoder.dft;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.jtransforms.fft.DoubleFFT_1D;

import cn.edu.thu.tsfile.common.utils.BytesUtils;
import cn.edu.thu.tsfile.common.utils.Pair;

/**
 * DFT encoder of double type
 * 
 * @author kangrong
 *
 */
public class DFTDoubleEncoder extends DFTEncoder<Double> {
    private DoubleFFT_1D fft;
    protected double[] cacheData;
    protected double[] overlapData;

    public DFTDoubleEncoder(int packLength, double rate, final float overlapRate) {
        super(packLength, rate, overlapRate);
        cacheData = new double[packLength];
        overlapData = new double[overlapStepLength];
    }

    @Override
    public void encode(double value, ByteArrayOutputStream out) {
        cacheData[writeIndex++] = value;
        if (writeIndex == packLength) {
            flush(out);
        }
    }

    @Override
    protected boolean checkStoreMode() {
        return rate < 0.6667;
    }

    @Override
    protected void writeFFTForward(int thresIndexes) throws IOException {
        List<Pair<Double, Integer>> sorted = new ArrayList<Pair<Double, Integer>>();
        // write to out
        if (thresIndexes != -1) {
            for (int i = 0; i < writeIndex; i++) {
                sorted.add(new Pair<Double, Integer>(cacheData[i], i));
            }
            Collections.sort(sorted, getComparator());
//            System.out.println("sort");
            
            Pair<Double, Integer> p;
            for (int i = 0; i < thresIndexes; i++) {
                p = sorted.get(i);
                out.write(BytesUtils.doubleToBytes(p.left));
                out.write(BytesUtils.intToBytes(p.right));
            }
        } else {
            for (int i = 0; i < writeIndex; i++) {
                out.write(BytesUtils.doubleToBytes(cacheData[i]));
            }
        }
    }



    @Override
    protected Comparator<Pair<Double, Integer>> getComparator() {
        return new Comparator<Pair<Double, Integer>>() {
            @Override
            public int compare(Pair<Double, Integer> o1, Pair<Double, Integer> o2) {
                return Double.valueOf(Math.abs(o2.left)).compareTo(Math.abs(o1.left));
            }
        };
    }

    @Override
    protected float[] getFFTForwardFrequency(int firstSize) {
        if (firstSize <= 0) {
            return new float[1];
        }
        double[] modulus = new double[writeIndex / 2];
        for (int i = 0; i < writeIndex / 2; i++)
            modulus[i] =
                    cacheData[2 * i] * cacheData[2 * i] + cacheData[2 * i + 1]
                            * cacheData[2 * i + 1];
        List<Pair<Double, Integer>> sorted = new ArrayList<Pair<Double, Integer>>();
        // first freq
        if (modulus.length >= 2) {
            if (modulus[0] > modulus[1])
                sorted.add(new Pair<Double, Integer>(modulus[0], 0));
            if (modulus[modulus.length - 1] > modulus[modulus.length - 2])
                sorted.add(new Pair<Double, Integer>(modulus[modulus.length - 1],
                        modulus.length - 1));
        }
        for (int i = 1; i < modulus.length - 1; i++) {
            if (modulus[i] > modulus[i - 1] && modulus[i] > modulus[i + 1])
                sorted.add(new Pair<Double, Integer>(modulus[i], i));
        }
        Collections.sort(sorted, getComparator());
        int retLength = Math.min(firstSize, sorted.size());
        float[] ret = new float[retLength];
        Pair<Double, Integer> p;
        for (int i = 0; i < retLength; i++) {
            p = sorted.get(i);
            ret[i] = (float) p.right / writeIndex;
        }
        return ret;
    }

    @Override
    protected void transform() {
        fft = new DoubleFFT_1D(writeIndex);
        fft.realForward(cacheData);
    }

    @Override
    protected void reset() {
        int delta = writeIndex - overlapStepLength;
        for (int i = 0; i < overlapStepLength; i++) {
            cacheData[i + delta] = overlapData[i];
        }
        writeIndex = overlapStepLength;
    }

    @Override
    protected void reserveOverlap() {
        int delta = writeIndex - overlapStepLength;
        for (int i = 0; i < overlapStepLength; i++) {
            overlapData[i] = cacheData[i + delta];
        }
    }

    @Override
    public int getOneItemMaxSize() {
        return isStoreInPair ? (int) (12 * rate) : 8;
    }

    @Override
    public long getMaxByteSize() {
        long ret = 1;
        if (isWriteMainFreq)
            ret += 4 + mainFreqNum * 4;
        if (isEncoding)
            ret += isStoreInPair ? writeIndex * rate * 12 : writeIndex * 8;
        return ret;
    }

}