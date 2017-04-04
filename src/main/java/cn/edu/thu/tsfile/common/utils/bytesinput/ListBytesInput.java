package cn.edu.thu.tsfile.common.utils.bytesinput;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * ListBytesInput is designed for saving byte array temporarily to avoid unnecessary byte array copy
 * in {@code OutputStream Writing}
 * 
 * @author kangrong
 *
 */
public class ListBytesInput extends BytesInput {
    private List<byte[]> slabs;
    private List<Integer> slabSizeList = new ArrayList<Integer>();
    private int size;

    public ListBytesInput(List<BytesInput> inputs) throws IOException {
        this.slabs = new ArrayList<byte[]>();
        this.size = 0;
        for (BytesInput input : inputs) {
            slabs.add(input.toByteArray());
            addSize(input.size());
        }
    }

    public ListBytesInput(PublicBAOS... baos) {
        slabs = new ArrayList<byte[]>();
        size = 0;
        for (PublicBAOS pbs : baos) {
            slabs.add(pbs.getBuf());
            addSize(pbs.size());
        }
    }

    /**
     * append given bytesInput array to this object. Exception is threw before changing the
     * ListBytesInput data, thus you needn't recover this ListBytesInput for exception
     * 
     * @param bis - BytesInput to be appended
     * @throws IOException
     */
    public void appendBytesInput(BytesInput... bis) throws IOException {
        for (BytesInput bi : bis) {
            slabs.add(bi.toByteArray());
            addSize(bi.size());
        }
    }

    /**
     * this method will not copy the buf in given PublicBAOS to this inputs, but just refer
     * parameter's buf to this inputs.
     * 
     * @param publicBAOS - to be appended
     */
    public void appendPublicBAOS(PublicBAOS... publicBAOS) {
        for (PublicBAOS pbs : publicBAOS) {
            slabs.add(pbs.getBuf());
            addSize(pbs.size());
        }
    }

    /**
     * this method will not copy the buf in given ListBytesInput to this inputs, but just refer
     * parameter's buf to this inputs.
     * 
     * @param lbInput - to be appended
     */
    public void appendListBytesInput(ListBytesInput... lbInput) {
        for (ListBytesInput bInput : lbInput) {
            for (int i = 0; i < bInput.slabs.size(); i++) {
                slabs.add(bInput.slabs.get(i));
                addSize(bInput.slabSizeList.get(i));
            }
        }
    }

    @Override
    public void writeAllTo(OutputStream out) throws IOException {
        for (int i = 0; i < slabs.size(); i++) {
            out.write(slabs.get(i), 0, slabSizeList.get(i));
        }
    }

    @Override
    public int size() {
        return size;
    }

    public void clear() {
        size = 0;
        slabs.clear();
        slabSizeList.clear();
    }

    private void addSize(int newSize) {
        size += newSize;
        slabSizeList.add(newSize);
    }

    /**
     * delete the last item in slabs and slabSizeList.
     */
    public void removeLast() {
        if(slabs.isEmpty())
            return;
        slabs.remove(slabs.size()-1);
        size -= slabSizeList.remove(slabSizeList.size()-1);
    }
}
