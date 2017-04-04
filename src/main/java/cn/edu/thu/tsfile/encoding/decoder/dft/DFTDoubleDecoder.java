package cn.edu.thu.tsfile.encoding.decoder.dft;

import java.io.IOException;
import java.io.InputStream;

import org.jtransforms.fft.DoubleFFT_1D;

import cn.edu.thu.tsfile.common.utils.BytesUtils;

/**
 * DFT decoder for double type
 * 
 * @author kangrong
 *
 */
public class DFTDoubleDecoder extends DFTDecoder<Double> {
    private DoubleFFT_1D fft;
    private double[] coefData;

    public DFTDoubleDecoder() {
        coefData = new double[1];
    }

    @Override
    public double readDouble(InputStream in) {
        return (double) readT(in);
    }

    @Override
    protected Double getNextValue() {
        return coefData[readIndex++];
    }

    @Override
    protected void readFFTForward(int thresIndes, InputStream in) throws IOException {
        if (coefData.length < packTotalSize)
            coefData = new double[packTotalSize];
        fft = new DoubleFFT_1D(packTotalSize);
        // read from in
        if (thresIndes >= 0) {
            for (int i = 0; i < coefData.length; i++)
                coefData[i] = 0;
            for (int i = 0; i < thresIndes; i++) {
                double coef = BytesUtils.readDouble(in);
                int index = BytesUtils.readInt(in);
                coefData[index] = coef;
            }
        } else {
            for (int i = 0; i < packTotalSize; i++) {
                coefData[i] = BytesUtils.readDouble(in);
            }
        }
        fft.realInverse(coefData, true);
    }

    @Override
    protected long calcLeftSize(int thresIndes) {
        return (thresIndes >= 0) ? (8 + 4) * thresIndes : packTotalSize * 8;
    }
}
