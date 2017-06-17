package cn.edu.thu.tsfile.common.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/***
 * This class is designed for maintaining several <code>ByteArrayOutputStream</code> and provides
 * functions including writeAllTo, size and reset.
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
     * Inputs an OutputStream as parameter. Writes the complete contents in <code>list</code> to
     * the specified output stream argument.
     *
     * @param out the output stream to write the data.
     * @throws IOException if an I/O error occurs.
     */
    public void writeAllTo(OutputStream out) throws IOException{
        for (ByteArrayOutputStream baos : list)
            baos.writeTo(out);
    }

    /**
     * get the total size of this class
     * @return total size
     */
    public int size(){
        return totalSize;
    }

    /**
     * Creates a new <code>PublicBAOS</code> which specified size is the current
     * total size and write the current contents in <code>list</code> into it.
     *
     * @return  the current contents of this class, as a byte array.
     * @throws IOException if an I/O error occurs.
     */
    public byte[] toByteArray() throws IOException {
        PublicBAOS baos = new PublicBAOS(totalSize);
        this.writeAllTo(baos);
        return baos.getBuf();
    }

    /**
     * Constructs ListByteArrayOutputStream using ByteArrayOutputStream.
     *
     * @param out the data source for constructing a <code>ListByteArrayOutputStream</code>
     * @return a new <code>ListByteArrayOutputStream</code> containing data in <code>out</code>.
     */
    public static ListByteArrayOutputStream from(ByteArrayOutputStream out) {
        return new ListByteArrayOutputStream(out);
    }

    /**
     * Appends a <code>ByteArrayOutputStream</code> into this class.
     * @param out a output stream to be appended.
     */
    public void append(ByteArrayOutputStream out) {
        list.add(out);
        totalSize += out.size();
    }

    /**
     * Resets the <code>list</code> and <code>totalSize</code> fields.
     */
    public void reset() {
        list.clear();
        totalSize = 0;
    }

    /**
     * A subclass extending <code>ByteArrayOutputStream</code>. It's used to return the byte array directly.
     * Note that the size of byte array is large than actual size of valid contents, thus it's used cooperating
     * with <code>size()</code> or <code>capacity = size</code>
     */
    private static final class PublicBAOS extends ByteArrayOutputStream {
        private PublicBAOS(int size) {
            super(size);
        }
        public byte[] getBuf() {
            return this.buf;
        }
    }

}
