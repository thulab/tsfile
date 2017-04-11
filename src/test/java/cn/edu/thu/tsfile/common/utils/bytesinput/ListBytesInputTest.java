package cn.edu.thu.tsfile.common.utils.bytesinput;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

/**
 * 
 * @author kangrong
 *
 */
public class ListBytesInputTest {
    private byte[] b1 = new byte[] {0, 1, 2};
    private byte[] b2 = new byte[] {3};
    private byte[] b3 = new byte[] {4, 5, 6, 7};
    private byte[] b4 = new byte[] {8, 9, 10};

    @Test
    public void testAppendBytesInput() {
        try {
            BytesInput.PublicBAOS pb1 = new BytesInput.PublicBAOS(b1);
            BytesInput bi1 = BytesInput.from(pb1);
            BytesInput.PublicBAOS pb2 = new BytesInput.PublicBAOS(b2);
            BytesInput bi2 = new ListBytesInput(pb2);
            BytesInput.PublicBAOS pb3 = new BytesInput.PublicBAOS(b3);
            BytesInput bi3 = BytesInput.from(pb3);
            BytesInput.PublicBAOS pb4;

            pb4 = new BytesInput.PublicBAOS(b4);

            BytesInput bi4 = new ListBytesInput(pb4);
            List<BytesInput> list = new ArrayList<BytesInput>();
            list.add(bi1);
            list.add(bi2);
            ListBytesInput ret = new ListBytesInput(list);
            byte[] r1 = ret.toByteArray();
            assertEquals(4, r1.length);
            for (int i = 0; i < r1.length; i++) {
                assertEquals(i, r1[i]);
            }
            ret.appendBytesInput(bi3);
            ret.appendBytesInput(bi4);
            byte[] r2 = ret.toByteArray();
            assertEquals(11, r2.length);
            for (int i = 0; i < r2.length; i++) {
                assertEquals(i, r2[i]);
            }
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    public void testErrorException() {
        BytesInput.PublicBAOS pb1 = null, pb2 = null, pb3 = null;
        try {
            pb1 = new BytesInput.PublicBAOS(b1);
            pb2 = new BytesInput.PublicBAOS(b2);
            pb3 = new BytesInput.PublicBAOS(b3);
        } catch (IOException e) {
            fail();
        }
        ListBytesInput list = new ListBytesInput(pb1, pb2, pb3);
        BytesInput errBi = new BytesInput() {
            @Override
            public void writeAllTo(OutputStream out) throws IOException {
                throw new IOException();
            }

            @Override
            public int size() {
                return 0;
            }
        };
        try {
            list.appendBytesInput(errBi);
            fail();
        } catch (IOException e) {
            list.removeLast();
            assertEquals(b1.length + b2.length,list.size());
            byte[] r1;
            try {
                r1 = list.toByteArray();
                assertEquals(b1.length + b2.length, r1.length);
                for (int i = 0; i < r1.length; i++) {
                    assertEquals(i, r1[i]);
                }
            } catch (IOException e1) {
                fail();
            }

        }
    }

    @Test
    public void testAppendPublicBAOS() {
        try {
            BytesInput.PublicBAOS pb1 = new BytesInput.PublicBAOS(b1);
            ListBytesInput ret = new ListBytesInput(pb1);

            BytesInput.PublicBAOS pb2 = new BytesInput.PublicBAOS(b2);
            BytesInput bi2 = new ListBytesInput(pb2);
            BytesInput.PublicBAOS pb3 = new BytesInput.PublicBAOS(b3);
            BytesInput bi3 = BytesInput.from(pb3);
            List<BytesInput> list = new ArrayList<BytesInput>();
            list.add(bi2);
            list.add(bi3);
            ListBytesInput addList = new ListBytesInput(list);
            ret.appendListBytesInput(addList);
            byte[] r1 = ret.toByteArray();
            assertEquals(8, r1.length);
            for (int i = 0; i < r1.length; i++) {
                assertEquals(i, r1[i]);
            }

            BytesInput.PublicBAOS pb4 = new BytesInput.PublicBAOS(b4);
            ret.appendPublicBAOS(pb4);
            byte[] r2 = ret.toByteArray();
            assertEquals(11, r2.length);
            for (int i = 0; i < r2.length; i++) {
                assertEquals(i, r2[i]);
            }
            ret.clear();
            assertEquals(0, ret.size());
        } catch (IOException e) {
            fail();
        }
    }

}
