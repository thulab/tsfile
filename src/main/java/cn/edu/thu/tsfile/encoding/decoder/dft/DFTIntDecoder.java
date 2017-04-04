package cn.edu.thu.tsfile.encoding.decoder.dft;

import java.io.InputStream;

/**
 * DFT decoder for int type
 * @author kangrong
 *
 */
public class DFTIntDecoder extends DFTFloatDecoder {

    @Override
    public int readInt(InputStream in) {
        return (int) (float) readT(in);
    }
}