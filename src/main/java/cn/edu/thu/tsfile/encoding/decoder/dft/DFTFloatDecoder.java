package cn.edu.thu.tsfile.encoding.decoder.dft;

import java.io.IOException;
import java.io.InputStream;

import org.jtransforms.fft.FloatFFT_1D;

import cn.edu.thu.tsfile.common.utils.BytesUtils;

/**
 * DFT decoder for float type
 * @author kangrong
 *
 */
public class DFTFloatDecoder extends DFTDecoder<Float> {
    private FloatFFT_1D fft;
    private float[] coefData;

    public DFTFloatDecoder() {
        coefData = new float[1];
    }

    @Override
    public float readFloat(InputStream in) {
        return (float) readT(in);
    }

    @Override
    protected Float getNextValue() {
        return coefData[readIndex++];
    }
    
    @Override
    protected void readFFTForward(int thresIndes, InputStream in) throws IOException {
        if (coefData.length < packTotalSize)
            coefData = new float[packTotalSize];
        fft = new FloatFFT_1D(packTotalSize);
        // read from in
        if (thresIndes >= 0) {
            for (int i = 0; i < coefData.length; i++)
                coefData[i] = 0;
            for (int i = 0; i < thresIndes; i++) {
                float coef = BytesUtils.readFloat(in);
                int index = BytesUtils.readInt(in);
                coefData[index] = coef;
            }
        } else {
            for (int i = 0; i < packTotalSize; i++) {
                coefData[i] = BytesUtils.readFloat(in);
            }
        }
        fft.realInverse(coefData, true);
    }

    @Override
    protected long calcLeftSize(int thresIndes) {
        return (thresIndes >= 0)?(4+4)*thresIndes:packTotalSize*4;
    }

}