package cn.edu.thu.tsfile.encoding.encoder.dft;

import java.io.ByteArrayOutputStream;

/**
 * DFT encoder of long type
 * 
 * @author kangrong
 *
 */
public class DFTLongEncoder extends DFTDoubleEncoder {
    public DFTLongEncoder(int packLength, double rate, final float overlapRate) {
        super(packLength, rate, overlapRate);
    }

    @Override
    public void encode(long value, ByteArrayOutputStream out) {
        cacheData[writeIndex++] = value;
        if (writeIndex == packLength) {
            flush(out);
        }
    }

}