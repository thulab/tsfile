package cn.edu.tsinghua.tsfile.compress;

import cn.edu.tsinghua.tsfile.common.exception.CompressionTypeNotSupportedException;
import cn.edu.tsinghua.tsfile.common.utils.ListByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.IOException;

/**
 * compress data according to type in schema
 */
public abstract class Compressor {
    public static Compressor getCompressor(String name) {
        return getCompressor(CompressionTypeName.valueOf(name));
    }

    public static Compressor getCompressor(CompressionTypeName name) {
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

    public abstract byte[] compress(ListByteArrayOutputStream ListByteArray) throws IOException;

    public abstract CompressionTypeName getCodecName();

    /**
     * NoCompressor will do nothing for data and return the input data directly.
     *
     * @author kangrong
     */
    static public class NoCompressor extends Compressor {

        @Override
        public byte[] compress(ListByteArrayOutputStream ListByteArray) throws IOException {
            return ListByteArray.toByteArray();
        }

        @Override
        public CompressionTypeName getCodecName() {
            return CompressionTypeName.UNCOMPRESSED;
        }
    }

    static public class SnappyCompressor extends Compressor {
        private static final Logger LOGGER = LoggerFactory.getLogger(SnappyCompressor.class);

        @Override
        public byte[] compress(ListByteArrayOutputStream listByteArray) throws IOException {
            if (listByteArray == null) {
                return null;
            }
            return Snappy.compress(listByteArray.toByteArray());
        }

        @Override
        public CompressionTypeName getCodecName() {
            return CompressionTypeName.SNAPPY;
        }
    }
}
