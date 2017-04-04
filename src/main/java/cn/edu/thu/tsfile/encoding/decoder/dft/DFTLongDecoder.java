package cn.edu.thu.tsfile.encoding.decoder.dft;

import java.io.InputStream;

/**
 * DFT decoder for long type
 * @author kangrong
 *
 */
public class DFTLongDecoder extends DFTDoubleDecoder {

    @Override
    public long readLong(InputStream in) {
        return (long) (double) readT(in);
    }
}