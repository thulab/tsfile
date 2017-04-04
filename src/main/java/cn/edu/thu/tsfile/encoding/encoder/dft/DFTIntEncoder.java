package cn.edu.thu.tsfile.encoding.encoder.dft;

import java.io.ByteArrayOutputStream;

/**
 * DFT encoder of integer type
 * 
 * @author kangrong
 *
 */
public class DFTIntEncoder extends DFTFloatEncoder {
    public DFTIntEncoder(int packLength, double rate, final float overlapRate) {
        super(packLength, rate, overlapRate);
    }

    @Override
    public void encode(int value, ByteArrayOutputStream out) {
        cacheData[writeIndex++] = value;
        if (writeIndex == packLength) {
            flush(out);
        }
    }
}