package cn.edu.thu.tsfile.common.utils;

import cn.edu.thu.tsfile.common.utils.ListByteArrayOutputStream;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * This class is used for testing functions of <code>ListByteOutputStream</code>.
 * @author kangrong
 */
public class ListByteOutputStreamTest {
    private byte[] b1 = new byte[]{0, 1, 2};
    private byte[] b2 = new byte[]{3};
    private byte[] b3 = new byte[]{4, 5, 6, 7};
    private byte[] b4 = new byte[]{8, 9, 10};

    private ByteArrayOutputStream s1;
    private ByteArrayOutputStream s2;
    private ByteArrayOutputStream s3;
    private ByteArrayOutputStream s4;
    private ByteArrayOutputStream total;

    @Before
    public void before() throws IOException {
        s1 = new ByteArrayOutputStream();
        s1.write(b1);
        s2 = new ByteArrayOutputStream();
        s2.write(b2);
        s3 = new ByteArrayOutputStream();
        s3.write(b3);
        s4 = new ByteArrayOutputStream();
        s4.write(b4);
        total = new ByteArrayOutputStream();
        total.write(b1);
        total.write(b2);
        total.write(b3);
        total.write(b4);
    }

    @Test
    public void testAppend() {
        try {
            ListByteArrayOutputStream listStream = new ListByteArrayOutputStream();
            listStream.append(s1);
            listStream.append(s2);
            listStream.append(s3);
            listStream.append(s4);
            assertEquals(11, listStream.size());
            byte[] ret = listStream.toByteArray();
            for (int i = 0; i < ret.length; i++) {
                assertEquals(i, ret[i]);
            }
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    public void testFrom() {
        try {
            ListByteArrayOutputStream listStream = ListByteArrayOutputStream.from(total);
            assertEquals(11, listStream.size());
            byte[] ret = listStream.toByteArray();
            for (int i = 0; i < ret.length; i++) {
                assertEquals(i, ret[i]);
            }
            listStream.reset();
            assertEquals(0, listStream.size());
        } catch (IOException e) {
            fail();
        }
    }


    @Test
    public void testToArray() {
        try {
            ListByteArrayOutputStream listStream = new ListByteArrayOutputStream(s1, s2, s3, s4);
            assertEquals(11, listStream.size());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            listStream.writeAllTo(out);
            byte[] ret = out.toByteArray();
            for (int i = 0; i < ret.length; i++) {
                assertEquals(i, ret[i]);
            }
            assertEquals(11, listStream.size());
            listStream.reset();
            assertEquals(0, listStream.size());
        } catch (IOException e) {
            fail();
        }
    }

}
