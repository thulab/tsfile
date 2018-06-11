package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.converter.IConverter;
import cn.edu.tsinghua.tsfile.format.Digest;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * For more information, see Digest in cn.edu.thu.tsfile.format package
 */
public class TsDigest {
    /**
     * Digest/statistics per row group and per page.
     */
    private Map<String, ByteBuffer> statistics;

    public TsDigest() {
    }

    public TsDigest(Map<String, ByteBuffer> statistics) {
        this.statistics = statistics;
    }

    public void setStatistics(Map<String, ByteBuffer> statistics) {
        this.statistics = statistics;
    }

    public Map<String, ByteBuffer> getStatistics() {
        return this.statistics;
    }

    public void addStatistics(String key, ByteBuffer value) {
        if (statistics == null) {
            statistics = new HashMap<>();
        }
        statistics.put(key, value);
    }

    @Override
    public String toString() {
        return statistics != null ? statistics.toString() : "";
    }

    public ByteBuffer byteBufferDeepCopy(ByteBuffer src) {
        ByteBuffer copy = ByteBuffer.allocate(src.remaining()).put(src.slice());
        copy.flip();
        return copy;
    }
}
