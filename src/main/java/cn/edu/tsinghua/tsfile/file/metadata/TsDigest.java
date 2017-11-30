package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.converter.IConverter;
import cn.edu.tsinghua.tsfile.format.Digest;

import java.nio.ByteBuffer;

/**
 * For more information, see Digest in cn.edu.thu.tsfile.format package
 */
public class TsDigest implements IConverter<Digest> {
    /**
     * Instead of long/double, we use ByteBuffer as type of max and min to improve versatility of
     * digest. Therefore, statistics of data whose type is int, long, double or flaot can be stored in digest.
     */
    public ByteBuffer max;
    public ByteBuffer min;

    public TsDigest() {
    }

    public TsDigest(ByteBuffer max, ByteBuffer min) {
        this.max = max;
        this.min = min;
    }

    @Override
    public String toString() {
        return String.format("max:%s, min:%s", max, min);
    }

    @Override
    public Digest convertToThrift() {
        Digest digest = new Digest();
        digest.setMax(max);
        digest.setMin(min);
        return digest;
    }

    @Override
    public void convertToTSF(Digest digestInThrift) {
        if (digestInThrift != null) {
            this.max = digestInThrift.bufferForMax();
            this.min = digestInThrift.bufferForMin();
        }
    }
}
