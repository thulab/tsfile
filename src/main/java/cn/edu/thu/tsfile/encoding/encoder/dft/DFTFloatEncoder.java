package cn.edu.thu.tsfile.encoding.encoder.dft;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.jtransforms.fft.FloatFFT_1D;

import cn.edu.thu.tsfile.common.utils.BytesUtils;
import cn.edu.thu.tsfile.common.utils.Pair;

/**
 * DFT encoder of float type
 * 
 * @author kangrong
 */
public class DFTFloatEncoder extends DFTEncoder<Float> {
    private FloatFFT_1D fft;
    protected float[] cacheData;
    protected float[] overlapData;

    public DFTFloatEncoder(int packLength, double rate, final float overlapRate) {
        super(packLength, rate, overlapRate);
        cacheData = new float[packLength];
        overlapData = new float[overlapStepLength];
    }

    @Override
    public void encode(float value, ByteArrayOutputStream out) {
        cacheData[writeIndex++] = value;
        if (writeIndex == packLength) {
            flush(out);
        }
    }

    @Override
    protected boolean checkStoreMode() {
        return rate < 0.5;
    }

    @Override
    protected void writeFFTForward(int thresIndexes) throws IOException {
        List<Pair<Float, Integer>> sorted = new ArrayList<Pair<Float, Integer>>();
        // write to out
        if (thresIndexes != -1) {
            for (int i = 0; i < writeIndex; i++) {
                sorted.add(new Pair<Float, Integer>(cacheData[i], i));
            }
            Collections.sort(sorted, getComparator());
            Pair<Float, Integer> p;
            for (int i = 0; i < thresIndexes; i++) {
                p = sorted.get(i);
                out.write(BytesUtils.floatToBytes(p.left));
                out.write(BytesUtils.intToBytes(p.right));
            }
        } else {
            for (int i = 0; i < writeIndex; i++) {
                out.write(BytesUtils.floatToBytes(cacheData[i]));
            }
        }
    }

    @Override
    protected Comparator<Pair<Float, Integer>> getComparator() {
        return new Comparator<Pair<Float, Integer>>() {
            @Override
            public int compare(Pair<Float, Integer> o1, Pair<Float, Integer> o2) {
                return Float.valueOf(Math.abs(o2.left)).compareTo(Math.abs(o1.left));
            }
        };
    }

    @Override
    public int getOneItemMaxSize() {
        return isStoreInPair ? (int) (8 * rate) : 4;
    }

    @Override
    public long getMaxByteSize() {
        long ret = 1;
        if (isWriteMainFreq)
            ret += 4 + mainFreqNum * 4;
        if (isEncoding)
            ret += isStoreInPair ? writeIndex * rate * 8 : writeIndex * 4;
        return ret;
    }

    /**
     * calculate FFT forward and return first frequency. Note that: cacheData has been transform to
     * FFT coefficients.
     * 
     * @param firstSize it's max returning size.
     * @return a float array which length <= firstSize
     * @throws IOException
     */
    @Override
    protected float[] getFFTForwardFrequency(int firstSize) {
        if (firstSize <= 0) {
            return new float[1];
        }
        float[] modulus = new float[writeIndex / 2];
        for (int i = 0; i < writeIndex / 2; i++)
            modulus[i] =
                    cacheData[2 * i] * cacheData[2 * i] + cacheData[2 * i + 1]
                            * cacheData[2 * i + 1];
        List<Pair<Float, Integer>> sorted = new ArrayList<Pair<Float, Integer>>();
        // first freq
        if (modulus.length >= 2) {
            if (modulus[0] > modulus[1])
                sorted.add(new Pair<Float, Integer>(modulus[0], 0));
            if (modulus[modulus.length - 1] > modulus[modulus.length - 2])
                sorted.add(new Pair<Float, Integer>(modulus[modulus.length - 1], modulus.length - 1));
        }
        for (int i = 1; i < modulus.length - 1; i++) {
            if (modulus[i] > modulus[i - 1] && modulus[i] > modulus[i + 1])
                sorted.add(new Pair<Float, Integer>(modulus[i], i));
        }
        Collections.sort(sorted, getComparator());
        int retLength = Math.min(firstSize, sorted.size());
        float[] ret = new float[retLength];
        Pair<Float, Integer> p;
        for (int i = 0; i < retLength; i++) {
            p = sorted.get(i);
            ret[i] = (float) p.right / writeIndex;
        }
        return ret;
    }

    @Override
    protected void transform() {
        fft = new FloatFFT_1D(writeIndex);
        fft.realForward(cacheData);
    }

    @Override
    protected void reset() {
        for (int i = 0; i < overlapStepLength; i++) {
            cacheData[i] = overlapData[i];
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
}
