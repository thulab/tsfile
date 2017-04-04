package cn.edu.thu.tsfile.encoding.encoder.dft;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.utils.BytesUtils;
import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.encoding.encoder.Encoder;
import cn.edu.thu.tsfile.file.metadata.enums.TSEncoding;


/**
 * 
 * DFT encodes time series to a list of frequency coefficients using Fast Fourier Transform. It
 * depends on a open-source library: jtransforms<br>
 * DFT has two store types: </p>
 * 1.coefficient pairs
 * <p>
 * 
 * <pre>
 * flag;
 * main frequency number;
 *      frequency list
 * packLength;
 * overlapLength;
 * ifType;
 * packSize;
 * coefficientSize;
 * a list of pair<index,coefficient>;
 * </pre>
 * 
 * </p>
 * <p>
 * 2.all coefficient array
 * <p>
 * 
 * <pre>
 * flag;
 * main frequency number;
 *      frequency list
 * packLength;
 * overlapLength;
 * packLength;
 * ifType;
 * packSize;
 * a list of coefficients
 * </pre>
 * 
 * </p>
 *
 * @author kangrong
 *
 * @param <T> Double or Float. {@code Long} data will be transformed to {@code double} and
 *        {@code integer} data will be transformed to {@code float}.
 */
public abstract class DFTEncoder<T> extends Encoder {
    protected static final Logger LOG = LoggerFactory.getLogger(DFTEncoder.class);
    private static final float defaultOverlapRate = 0.4f;
    private static final int defaultPackLength = 10000;

    /**
     * pack length
     */
    protected final int packLength;
    /**
     * compression rate
     */
    protected final double rate;

    /**
     * the length of overlap
     */
    protected int overlapStepLength;

    /**
     * if rate is more than 0.5 in float or more than 0.667 in double, all data points will be
     * stored in list format. Otherwise, two lists of indexes and coefficients will be stored.
     */
    protected boolean isStoreInPair;

    /**
     * true if it need to store main frequency
     */
    protected boolean isWriteMainFreq = false;
    /**
     * true if save the coefficients for encoding and decoding data
     */
    protected boolean isEncoding = true;
    /**
     * the count of main frequency to store for each pack
     */
    protected int mainFreqNum = 1;

    protected int writeIndex;

    protected ByteArrayOutputStream out;

    /**
     * construct a DFTEncoder.
     * 
     * @param packLength - the number of data which should be transform
     * @param rate - the rate that remained in FFT forward coefficients which is between 0~1
     * @param overlapRate - the rate of overlap length
     */
    protected DFTEncoder(final int packLength, final double rate, final float overlapRate) {
        super(TSEncoding.DFT);
        if (packLength <= 0) {
            LOG.warn(
                    "DFT path length {} cannot be less than or equal to 0, set to be default length:{}",
                    packLength, defaultPackLength);
            this.packLength = defaultPackLength;
        } else
            this.packLength = packLength;
        this.rate = rate;
        this.overlapStepLength = (int) (packLength * overlapRate);
        if (overlapStepLength >= packLength) {
            LOG.warn(
                    "DFT overlap rate {} cannot be large than or equal to 1, set as default rate {}",
                    overlapRate, defaultOverlapRate);
            overlapStepLength = (int) (packLength * defaultOverlapRate);
        }
        isStoreInPair = checkStoreMode();
    }

    public void setMainFreqNum(int mainFreqNum) {
        this.mainFreqNum = mainFreqNum;
    }

    public void setIsWriteMainFreq(boolean isWriteMainFreq) {
        this.isWriteMainFreq = isWriteMainFreq;
    }

    public void setIsEncoding(boolean isEncoding) {
        this.isEncoding = isEncoding;
    }

    private byte setFlag(byte src, int offset, boolean value) {
        return BytesUtils.setByteN(src, offset, value ? 1 : 0);
    }

    @Override
    public void flush(ByteArrayOutputStream out) {
        try {
            flushBlockBuffer(out);
        } catch (IOException e) {
            LOG.error("flush DFT encoder failed, {}", e.getMessage());
        }
    }

    private void flushBlockBuffer(ByteArrayOutputStream out) throws IOException {
        if (writeIndex == overlapStepLength)
            return;
        this.out = out;
        if (writeIndex > overlapStepLength)
            reserveOverlap();
        writeToOut();
        if (writeIndex > overlapStepLength)
            reset();
    }

    protected void writeToOut() throws IOException {
        // write header
        transform();
        byte flag = 0;
        flag = setFlag(flag, EncoderFlagOffset.isWriteMainFreq, isWriteMainFreq);
        flag = setFlag(flag, EncoderFlagOffset.isEncoding, isEncoding);
        flag = setFlag(flag, EncoderFlagOffset.isStoreInPair, isStoreInPair);
        out.write(flag);
        if (isWriteMainFreq) {
            float[] mainFreqs = getFFTForwardFrequency(mainFreqNum);
            LOG.debug("main frequency:{}", Arrays.toString(mainFreqs));
            out.write(BytesUtils.intToBytes(mainFreqs.length));
            for (float f : mainFreqs)
                out.write(BytesUtils.floatToBytes(f));
        }
        if (isEncoding) {
            out.write(BytesUtils.intToBytes(writeIndex));
            out.write(BytesUtils.intToBytes(overlapStepLength));
            if (isStoreInPair) {
                int thresIndexes = (int) (writeIndex * rate);
                // if storing in pair, the index's length should be 1 at least.
                if (thresIndexes <= 0)
                    thresIndexes = 1;
                out.write(BytesUtils.intToBytes(thresIndexes));
                writeFFTForward(thresIndexes);

            } else {
                writeFFTForward(-1);
            }
        }
    }

    /**
     * return store mode
     * 
     * @return true if store in pair
     */
    protected abstract boolean checkStoreMode();

    /**
     * calculate FFT forward and return first frequency.
     * 
     * @param firstSize it's max returning size.
     * @return a float array which length <= firstSize
     */
    protected abstract float[] getFFTForwardFrequency(int firstSize);

    /**
     * reserve the overlap partition of current data for next pack
     */
    protected abstract void reserveOverlap();

    /**
     * recover the overlap partition for next pack
     */
    protected abstract void reset();

    /**
     * execute FFT to take a in-place transformation
     */
    protected abstract void transform();

    /**
     * write the transformed coefficients to output steam
     * 
     * @param thresIndexes - coefficient start index to write, if it is -1, not write into pair for
     *        sorting by key
     * @throws IOException
     */
    protected abstract void writeFFTForward(int thresIndexes) throws IOException;

    /**
     * whatever float or double, return a comparator for TreeMap sorting
     * 
     * @return a comparator sorting number in descending order
     */
    protected abstract Comparator<Pair<T, Integer>> getComparator();

}
