package cn.edu.thu.tsfile.common.utils;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import cn.edu.thu.tsfile.common.utils.bytesinput.BytesInput;
import org.junit.Test;

/**
 * 
 * @author kangrong
 *
 */
public class BytesInputTest {
  private Random r = new Random(System.currentTimeMillis());

  @Test
  public void testWriteAllTo() throws IOException {
    int i1 = 12;
    boolean b1 = true;
    double d1 = 123135.154d;
    List<byte[]> src = new ArrayList<byte[]>();
    src.add(BytesUtils.intToBytes(i1));
    src.add(BytesUtils.boolToBytes(b1));
    src.add(BytesUtils.doubleToBytes(d1));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(BytesUtils.concatByteArrayList(src));
    BytesInput bytesInput = BytesInput.from(out);
    assertEquals(13, bytesInput.size());
    // test ToByteArray
    byte[] byteInputArray = bytesInput.toByteArray();
    // test writeTo
    ByteArrayOutputStream retOut = new ByteArrayOutputStream();
    bytesInput.writeAllTo(retOut);
    byte[] writeToArray = retOut.toByteArray();
    assertEquals(writeToArray.length, byteInputArray.length);
    for (int i = 0; i < writeToArray.length; i++) {
      assertEquals(writeToArray[i], byteInputArray[i]);
    }
    // test correctness
    InputStream in = new ByteArrayInputStream(byteInputArray);
    assertEquals(i1, BytesUtils.readInt(in));
    assertEquals(b1, BytesUtils.readBool(in));
    assertEquals(d1, BytesUtils.readDouble(in), CommonTestConstant.double_min_delta);
  }

  @Test
  public void testConcat() throws IOException {
    int i1 = r.nextInt();
    boolean b1 = r.nextBoolean();
    double d1 = r.nextDouble();
    List<byte[]> src = new ArrayList<byte[]>();
    src.add(BytesUtils.intToBytes(i1));
    src.add(BytesUtils.boolToBytes(b1));
    src.add(BytesUtils.doubleToBytes(d1));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(BytesUtils.concatByteArrayList(src));
    BytesInput bytesInput1 = BytesInput.from(out);

    int i2 = r.nextInt();
    boolean b2 = r.nextBoolean();
    double d2 = r.nextDouble();
    src = new ArrayList<byte[]>();
    src.add(BytesUtils.intToBytes(i2));
    src.add(BytesUtils.boolToBytes(b2));
    src.add(BytesUtils.doubleToBytes(d2));
    out = new ByteArrayOutputStream();
    out.write(BytesUtils.concatByteArrayList(src));
    BytesInput bytesInput2 = BytesInput.from(out);

    BytesInput con = BytesInput.concat(bytesInput1, bytesInput2);

    assertEquals(13 * 2, con.size());
    // test ToByteArray
    byte[] byteInputArray = con.toByteArray();
    // test writeTo
    ByteArrayOutputStream retOut = new ByteArrayOutputStream();
    con.writeAllTo(retOut);
    byte[] writeToArray = retOut.toByteArray();
    assertEquals(writeToArray.length, byteInputArray.length);
    for (int i = 0; i < writeToArray.length; i++) {
      assertEquals(writeToArray[i], byteInputArray[i]);
    }
    // test correctness
    InputStream in = new ByteArrayInputStream(byteInputArray);
    assertEquals(i1, BytesUtils.readInt(in));
    assertEquals(b1, BytesUtils.readBool(in));
    assertEquals(d1, BytesUtils.readDouble(in), CommonTestConstant.double_min_delta);
    assertEquals(i2, BytesUtils.readInt(in));
    assertEquals(b2, BytesUtils.readBool(in));
    assertEquals(d2, BytesUtils.readDouble(in), CommonTestConstant.double_min_delta);

  }

}
