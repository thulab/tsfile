package cn.edu.tsinghua.ts2file.timesegment.write.segment;

import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.common.utils.ListByteArrayOutputStream;
import cn.edu.tsinghua.tsfile.common.utils.PublicBAOS;
import cn.edu.tsinghua.tsfile.common.utils.ReadWriteStreamUtils;
import cn.edu.tsinghua.tsfile.encoding.encoder.Encoder;
import java.io.IOException;
import java.math.BigDecimal;

/**
 * Created by qmm on 18/3/14.
 */
public class ValueWriter {
  // start time
  private Encoder startTimeEncoder;
  private PublicBAOS startTimeOut;
  // end time
  private Encoder endTimeEncoder;
  private PublicBAOS endTimeOut;
  // value
  private Encoder valueEncoder;
  private PublicBAOS valueOut;

  private PublicBAOS startTimeSizeOut;
  private PublicBAOS endTimeSizeOut;

  public ValueWriter() {
    this.startTimeOut = new PublicBAOS();
    this.endTimeOut = new PublicBAOS();
    this.valueOut = new PublicBAOS();
    this.startTimeSizeOut = new PublicBAOS();
    this.endTimeSizeOut = new PublicBAOS();
  }

  public void write(long startTime, long endTime, boolean value) throws IOException {
    startTimeEncoder.encode(startTime, startTimeOut);
    endTimeEncoder.encode(endTime, endTimeOut);
    valueEncoder.encode(value, valueOut);
  }

  public void write(long startTime, long endTime, short value) throws IOException {
    startTimeEncoder.encode(startTime, startTimeOut);
    endTimeEncoder.encode(endTime, endTimeOut);
    valueEncoder.encode(value, valueOut);
  }

  public void write(long startTime, long endTime, int value) throws IOException {
    startTimeEncoder.encode(startTime, startTimeOut);
    endTimeEncoder.encode(endTime, endTimeOut);
    valueEncoder.encode(value, valueOut);
  }

  public void write(long startTime, long endTime, long value) throws IOException {
    startTimeEncoder.encode(startTime, startTimeOut);
    endTimeEncoder.encode(endTime, endTimeOut);
    valueEncoder.encode(value, valueOut);
  }

  public void write(long startTime, long endTime, float value) throws IOException {
    startTimeEncoder.encode(startTime, startTimeOut);
    endTimeEncoder.encode(endTime, endTimeOut);
    valueEncoder.encode(value, valueOut);
  }

  public void write(long startTime, long endTime, double value) throws IOException {
    startTimeEncoder.encode(startTime, startTimeOut);
    endTimeEncoder.encode(endTime, endTimeOut);
    valueEncoder.encode(value, valueOut);
  }

  public void write(long startTime, long endTime, BigDecimal value) throws IOException {
    startTimeEncoder.encode(startTime, startTimeOut);
    endTimeEncoder.encode(endTime, endTimeOut);
    valueEncoder.encode(value, valueOut);
  }

  public void write(long startTime, long endTime, Binary value) throws IOException {
    startTimeEncoder.encode(startTime, startTimeOut);
    endTimeEncoder.encode(endTime, endTimeOut);
    valueEncoder.encode(value, valueOut);
  }

  /**
   * flush all data remained in encoders.
   *
   * @throws IOException
   */
  private void prepareEndWriteOnePage() throws IOException {
    startTimeEncoder.flush(startTimeOut);
    endTimeEncoder.flush(endTimeOut);
    valueEncoder.flush(valueOut);
    startTimeOut.flush();
    endTimeOut.flush();
    valueOut.flush();
  }

  /**
   * getBytes return data what it has been written in form of <code>ListByteArrayOutputStream</code>.
   *
   * @return - list byte array output stream containing time size, time stream and value stream.
   * @throws IOException exception in IO
   */
  public ListByteArrayOutputStream getBytes() throws IOException {
    prepareEndWriteOnePage();
    ReadWriteStreamUtils.writeUnsignedVarInt(startTimeOut.size(), startTimeSizeOut);
    ReadWriteStreamUtils.writeUnsignedVarInt(endTimeOut.size(), endTimeSizeOut);
    return new ListByteArrayOutputStream(startTimeSizeOut, startTimeOut, endTimeSizeOut, endTimeOut, valueOut);
  }

  /**
   * calculate max possible memory size it occupies, including time outputStream and value outputStream
   *
   * @return allocated size in time, value and outputStream
   */
  public long estimateMaxMemSize() {
    return startTimeOut.size() + endTimeOut.size() + valueOut.size() + startTimeEncoder
        .getMaxByteSize() + endTimeEncoder.getMaxByteSize() + valueEncoder.getMaxByteSize();
  }

  /**
   * reset data in ByteArrayOutputStream
   */
  public void reset() {
    startTimeOut.reset();
    endTimeOut.reset();
    valueOut.reset();
    startTimeSizeOut.reset();
    endTimeSizeOut.reset();
  }

  public void setStartTimeEncoder(Encoder startTimeEncoder) {
    this.startTimeEncoder = startTimeEncoder;
  }

  public void setEndTimeEncoder(Encoder endTimeEncoder) {
    this.endTimeEncoder = endTimeEncoder;
  }

  public void setValueEncoder(Encoder encoder) {
    this.valueEncoder = encoder;
  }
}
