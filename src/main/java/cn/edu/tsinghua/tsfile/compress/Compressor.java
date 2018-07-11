package cn.edu.tsinghua.tsfile.compress;

import cn.edu.tsinghua.tsfile.common.exception.CompressionTypeNotSupportedException;
import cn.edu.tsinghua.tsfile.common.utils.ByteBufferUtil;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * compress data according to type in schema
 */
public abstract class Compressor {
    public static Compressor getCompressor(String name) {
        return getCompressor(CompressionType.valueOf(name));
    }

    public static Compressor getCompressor(CompressionType name) {
        if (name == null) {
            throw new CompressionTypeNotSupportedException("NULL");
        }
        switch (name) {
            case UNCOMPRESSED:
                return new NoCompressor();
            case SNAPPY:
                return new SnappyCompressor();
            default:
                throw new CompressionTypeNotSupportedException(name.toString());
        }
    }

    public abstract byte[] compress(ByteBuffer data) throws IOException;

    public abstract CompressionType getCodecName();

    /**
     * NoCompressor will do nothing for data and return the input data directly.
     *
     * @author kangrong
     */
    static public class NoCompressor extends Compressor {

        @Override
        public byte[] compress(ByteBuffer data) throws IOException {//FIXME why do we use bytes[] rather than bytebuffer.
            return ByteBufferUtil.getArray(data);
        }

        @Override
        public CompressionType getCodecName() {
            return CompressionType.UNCOMPRESSED;
        }
    }

    static public class SnappyCompressor extends Compressor {
        private static final Logger LOGGER = LoggerFactory.getLogger(SnappyCompressor.class);

        @Override
        public byte[] compress(ByteBuffer data) throws IOException {
            if (data == null) {
                return null;
            }
            return Snappy.compress(ByteBufferUtil.getArray(data));
        }

        @Override
        public CompressionType getCodecName() {
            return CompressionType.SNAPPY;
        }
    }
}
