package cn.edu.thu.tsfile.timeseries.write.series;

import java.io.IOException;
import java.math.BigDecimal;

import cn.edu.thu.tsfile.common.utils.Binary;
import cn.edu.thu.tsfile.common.utils.ReadWriteStreamUtils;
import cn.edu.thu.tsfile.common.utils.bytesinput.BytesInput;
import cn.edu.thu.tsfile.encoding.encoder.Encoder;
import cn.edu.thu.tsfile.common.utils.bytesinput.ListBytesInput;

/**
 * This function is used to write time-value into a time series. It consists of a time encoder, a
 * value encoder and respective OutputStream.
 * 
 * @author kangrong
 *
 */
public class ValueWriter {
    // time
    private Encoder timeEncoder;
    private BytesInput.PublicBAOS timeOut;
    // value
    private Encoder valueEncoder;
    private BytesInput.PublicBAOS valueOut;


    public ValueWriter() {
        this.timeOut = new BytesInput.PublicBAOS();
        this.valueOut = new BytesInput.PublicBAOS();
    }

    public void write(long time, boolean value) throws IOException {
        timeEncoder.encode(time, timeOut);
        valueEncoder.encode(value, valueOut);
    }

    public void write(long time, short value) throws IOException {
        timeEncoder.encode(time, timeOut);
        valueEncoder.encode(value, valueOut);
    }

    public void write(long time, int value) throws IOException {
        timeEncoder.encode(time, timeOut);
        valueEncoder.encode(value, valueOut);
    }

    public void write(long time, long value) throws IOException {
        timeEncoder.encode(time, timeOut);
        valueEncoder.encode(value, valueOut);
    }

    public void write(long time, float value) throws IOException {
        timeEncoder.encode(time, timeOut);
        valueEncoder.encode(value, valueOut);
    }

    public void write(long time, double value) throws IOException {
        timeEncoder.encode(time, timeOut);
        valueEncoder.encode(value, valueOut);
    }

    public void write(long time, BigDecimal value) throws IOException {
        timeEncoder.encode(time, timeOut);
        valueEncoder.encode(value, valueOut);
    }

    public void write(long time, Binary value) throws IOException {
        timeEncoder.encode(time, timeOut);
        valueEncoder.encode(value, valueOut);
    }

    /**
     * flush all data remained in encoders.
     * 
     * @throws IOException
     */
    private void prepareEndWriteOnePage() throws IOException {
        timeEncoder.flush(timeOut);
        valueEncoder.flush(valueOut);
        timeOut.flush();
        valueOut.flush();
    }

    /**
     * getBytes return data what it has been written in form of BytesInput.
     * 
     * @return - byte array output stream packaged in BytesInput
     */
    public ListBytesInput getBytes() throws IOException {
        ListBytesInput resBytesInput;
        prepareEndWriteOnePage();
        resBytesInput = new ListBytesInput(new BytesInput.PublicBAOS());
        BytesInput.PublicBAOS timeMetadata = new BytesInput.PublicBAOS();
        ReadWriteStreamUtils.writeUnsignedVarInt(timeOut.size(), timeMetadata);
        resBytesInput.appendPublicBAOS(timeMetadata, timeOut, valueOut);
        return resBytesInput;
    }

    /**
     * calculate max possible memory size it occupies, including time outputStream and value outputStream
     * 
     * @return allocated size in time, value and outputStream
     */
    public long estimateMaxMemSize() {
        return timeOut.size() + valueOut.size()+timeEncoder.getMaxByteSize()+valueEncoder.getMaxByteSize();
    }

    /**
     * reset data in ByteArrayOutputStream
     */
    public void reset() {
        timeOut = new BytesInput.PublicBAOS();
        valueOut = new BytesInput.PublicBAOS();
    }

    public void setTimeEncoder(Encoder encoder) {
        this.timeEncoder = encoder;
    }

    public void setValueEncoder(Encoder encoder) {
        this.valueEncoder = encoder;
    }

}
