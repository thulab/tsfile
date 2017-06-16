package cn.edu.thu.tsfile.common.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

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
public class ListByteArrayOutputStream {
    private List<ByteArrayOutputStream> list;
    private int totalSize = 0;

    public ListByteArrayOutputStream(ByteArrayOutputStream ... param) {
        list = new ArrayList<>();
        for (ByteArrayOutputStream out : param){
            list.add(out);
            totalSize += out.size();
        }
    }

    /**
     * input an OutputStream as parameter. BytesInput output its data to this OutputStream
     *
     * @param out
     * @throws IOException
     */
    public void writeAllTo(OutputStream out) throws IOException{
        for (ByteArrayOutputStream baos : list)
            baos.writeTo(out);
    }

    public int size(){
        return totalSize;
    }

    /**
     * using BAOS to convert data itself to byte array
     *
     * @return converting result
     * @throws IOException
     */
    public byte[] toByteArray() throws IOException {
        PublicBAOS baos = new PublicBAOS(totalSize);
        this.writeAllTo(baos);
        return baos.getBuf();
    }

    /**
     * construct BytesInput with ByteArrayOutputStream
     *
     * @param out
     * @return
     */
    public static ListByteArrayOutputStream from(ByteArrayOutputStream out) {
        return new ListByteArrayOutputStream(out);
    }

    public void append(ByteArrayOutputStream out) {
        list.add(out);
        totalSize += out.size();
    }

    public void reset() {
        list.clear();
        totalSize = 0;
    }

    private static final class PublicBAOS extends ByteArrayOutputStream {
        private PublicBAOS(int size) {
            super(size);
        }
        public byte[] getBuf() {
            return this.buf;
        }
    }

}
