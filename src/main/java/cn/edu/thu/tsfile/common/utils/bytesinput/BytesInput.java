package cn.edu.thu.tsfile.common.utils.bytesinput;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/***
 * BytesInput is a abstract class encapsulating several operations related to OutputStream with
 * decorator pattern. BytesInput can concat multiple BytesInput and convert OutputStream to bytes.
 * BytesInput supports multiple constructor like Integer array with IntegerBytesInput. BytesInput
 * stores data temporarily in form of OutputStream or object array for outputting into OutputStream.
 * Its subclass should override writeAllTo and size.
 *
 * This class is inspired by org.apache.parquet.bytes.BytesInput
 *
 * @author kangrong
 *
 */
public abstract class BytesInput {
    /**
     * input an OutputStream as parameter. BytesInput output its data to this OutputStream
     * 
     * @param out
     * @throws IOException
     */
    abstract public void writeAllTo(OutputStream out) throws IOException;

    abstract public int size();

    /**
     * using BAOS to convert data itself to byte array
     * 
     * @return converting result
     * @throws IOException
     */
    public byte[] toByteArray() throws IOException {
        PublicBAOS baos = new PublicBAOS((int) size());
        this.writeAllTo(baos);
        return baos.getBuf();
    }

    /**
     * concat a list of BytesInput using ListBytesInput
     * 
     * @param inputs : a list of BytesInput
     * @return
     * @throws IOException
     */
    public static BytesInput concat(BytesInput... inputs) throws IOException {
        return new ListBytesInput(Arrays.asList(inputs));
    }

    /**
     * construct BytesInput with ByteArrayOutputStream
     * 
     * @param out
     * @return
     */
    public static BytesInput from(ByteArrayOutputStream out) {
        return new BAOSBytesInput(out);
    }

    /**
     * maintains a ByteArrayOutputStream variable arrayOut.
     * 
     * @author kangrong
     *
     */
    private static class BAOSBytesInput extends BytesInput {
        private final ByteArrayOutputStream arrayOut;

        private BAOSBytesInput(ByteArrayOutputStream arrayOut) {
            this.arrayOut = arrayOut;
        }

        @Override
        public void writeAllTo(OutputStream out) throws IOException {
            arrayOut.writeTo(out);
        }

        @Override
        public int size() {
            return arrayOut.size();
        }

    }

    /**
     * BAOS extends ByteArrayOutputStream. It provide getBuf to get protected variable buf in
     * ByteArrayOutputStream. <b>Note that</b>, be careful to use {@code reset()} function in
     * PublicBAOS, since buf of PublicBAOS is usually maintained for flushing later, if you call
     * {@code reset()} before others flushing, it causes error.
     * 
     * @author kangrong
     *
     */
    public static final class PublicBAOS extends ByteArrayOutputStream {
        private PublicBAOS(int size) {
            super(size);
        }

        public PublicBAOS() {

        }

        /**
         * This constructor copy given parameter {@code bs} to its buf.
         * 
         * @param bs - given byte array to construct PublicBAOS
         * @throws IOException
         */
        public PublicBAOS(byte[] bs) throws IOException {
            super(bs.length);
            super.write(bs);
        }

        public byte[] getBuf() {
            return this.buf;
        }
    }

}
