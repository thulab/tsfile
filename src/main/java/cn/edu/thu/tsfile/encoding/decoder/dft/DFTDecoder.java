package cn.edu.thu.tsfile.encoding.decoder.dft;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfile.encoding.encoder.dft.DFTEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.utils.BytesUtils;
import cn.edu.thu.tsfile.encoding.decoder.Decoder;
import cn.edu.thu.tsfile.encoding.encoder.dft.EncoderFlagOffset;
import cn.edu.thu.tsfile.file.metadata.enums.TSEncoding;

/**
 * DFT decoder
 * 
 * @see DFTEncoder DFTEncoder
 * @author kangrong
 *
 * @param <T> including Float, Double, Integer and Long
 */
public abstract class DFTDecoder<T> extends Decoder {
    private static final Logger LOG = LoggerFactory.getLogger(DFTDecoder.class);

    protected int packTotalSize;
    protected boolean isStoreInPair;
    protected boolean isWriteMainFreq = false;
    protected boolean isEncoding = true;
    protected int lastOverlap = 0;

    protected int readIndex = 0;

    public DFTDecoder() {
        super(TSEncoding.DFT);
    }

    protected abstract T getNextValue();
    
    /**
     * extract frequency data from given input stream
     * @param in - encoded input stream
     * @return
     * @throws IOException
     */
    public List<float[]> getMainFrequency(InputStream in) throws IOException {
        List<float[]> ret = new ArrayList<float[]>();
        while(in.available()>0){
            float[] temp = getPackMainFrequency(in);
            ret.add(temp);
        }   
        return ret;
    }
    
    private boolean getFlag(byte src, int offset) {
        return BytesUtils.getByteN(src, offset) == 1;
    }
    
    protected void readFlag(InputStream in) throws IOException {
        byte flag = (byte)in.read();
        isWriteMainFreq = getFlag(flag, EncoderFlagOffset.isWriteMainFreq);
        isEncoding = getFlag(flag, EncoderFlagOffset.isEncoding);
        isStoreInPair = getFlag(flag, EncoderFlagOffset.isStoreInPair);
    }
    
    public float[] getPackMainFrequency(InputStream in) throws IOException {
        float[] retFreqs = {0};
        readFlag(in);
        if(isWriteMainFreq){
            int freqLength = BytesUtils.readInt(in);
            retFreqs = new float[freqLength];
            for(int i = 0; i < freqLength; i++)
                retFreqs[i] = BytesUtils.readFloat(in);
        }
        if(isEncoding){
            //total pack length for 4 bytes and overlap for 4 bytes
            in.skip(8);
            //skip the left InputStream
            int thresIndes = -1;
            if(isStoreInPair){
                thresIndes = BytesUtils.readInt(in);
            }
            //4 is the length of overlap
            in.skip(calcLeftSize(thresIndes));
        }
        return retFreqs;
    }
    
    abstract protected long calcLeftSize(int thresIndes);

    /**
     * if remaining data has been run out, load next pack from InputStream
     * 
     * @param in
     * @throws IOException
     */
    protected void loadIntBatch(InputStream in) throws IOException {
        readFlag(in);
        if(isWriteMainFreq){
            int freqLength = BytesUtils.readInt(in);
            in.skip(freqLength*4);
        }
        if(!isEncoding){
            LOG.error("encoding information hasn't been saved! exit.");
            System.exit(1);
        }
        packTotalSize = BytesUtils.readInt(in);
        readIndex = lastOverlap;
        lastOverlap = BytesUtils.readInt(in);
        if (isStoreInPair) {
            int thresIndes = BytesUtils.readInt(in);
            readFFTForward(thresIndes, in);
        } else {
            readFFTForward(-1, in);
        }
    }

    /**
     * read coefficients from inputStream, if thresIndexes > 0, type is in-pair
     * 
     * @param thresIndes if thresIndexes > 0, type is in-pair,otherwise it's all coefficients list
     * @throws IOException
     */
    protected abstract void readFFTForward(int thresIndes, InputStream in) throws IOException;

    public boolean hasNext(InputStream in) throws IOException {
        return (readIndex < packTotalSize) || in.available() > 0;
    }

    protected T readT(InputStream in) {
        if (readIndex == packTotalSize) {
            try {
                loadIntBatch(in);
            } catch (IOException e) {
                LOG.error("meet IOException when load batch from InputStream, exit, {}", e.getMessage());
                return null;
            }
        }
        return getNextValue();
    }
}
