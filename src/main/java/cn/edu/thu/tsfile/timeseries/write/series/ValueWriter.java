package cn.edu.thu.tsfile.timeseries.write.series;

import java.io.IOException;
import java.math.BigDecimal;

import cn.edu.thu.tsfile.common.utils.Binary;
import cn.edu.thu.tsfile.common.utils.ReadWriteStreamUtils;
import cn.edu.thu.tsfile.common.utils.bytesinput.ListBytesInput;
import cn.edu.thu.tsfile.encoding.encoder.Encoder;
import cn.edu.thu.tsfile.common.utils.bytesinput.BytesInput;

/**
 * This function is used to write time-value into a time series. It consists of a time encoder, a
 * value encoder, a frequency encoder if necessary and respective OutputStream.
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

    // frequency
    private Encoder freqEncoder = null;
    private BytesInput.PublicBAOS freout;

    public ValueWriter() {
        this.timeOut = new BytesInput.PublicBAOS();
        this.valueOut = new BytesInput.PublicBAOS();
        this.freout = new BytesInput.PublicBAOS();
    }

    public void write(long time, boolean value) throws IOException {
        timeEncoder.encode(time, timeOut);
        valueEncoder.encode(value, valueOut);
        if (freqEncoder != null) {
            freqEncoder.encode(value, freout);
        }
    }

    public void write(long time, short value) throws IOException {
        timeEncoder.encode(time, timeOut);
        valueEncoder.encode(value, valueOut);
        if (freqEncoder != null) {
            freqEncoder.encode(value, freout);
        }
    }

    public void write(long time, int value) throws IOException {
        timeEncoder.encode(time, timeOut);
        valueEncoder.encode(value, valueOut);
        if (freqEncoder != null) {
            freqEncoder.encode(value, freout);
        }
    }

    public void write(long time, long value) throws IOException {
        timeEncoder.encode(time, timeOut);
        valueEncoder.encode(value, valueOut);
        if (freqEncoder != null) {
            freqEncoder.encode(value, freout);
        }
    }

    public void write(long time, float value) throws IOException {
        timeEncoder.encode(time, timeOut);
        valueEncoder.encode(value, valueOut);
        if (freqEncoder != null) {
            freqEncoder.encode(value, freout);
        }
    }

    public void write(long time, double value) throws IOException {
        timeEncoder.encode(time, timeOut);
        valueEncoder.encode(value, valueOut);
        if (freqEncoder != null) {
            freqEncoder.encode(value, freout);
        }
    }

    public void write(long time, BigDecimal value) throws IOException {
        timeEncoder.encode(time, timeOut);
        valueEncoder.encode(value, valueOut);
        if (freqEncoder != null) {
            freqEncoder.encode(value, freout);
        }
    }

    public void write(long time, Binary value) throws IOException {
        timeEncoder.encode(time, timeOut);
        valueEncoder.encode(value, valueOut);
        if (freqEncoder != null) {
            freqEncoder.encode(value, freout);
        }
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
        if (freqEncoder != null) {
            freqEncoder.flush(freout);
            freout.flush();
        }
    }

    /**
     * getBytes return data what it has been written in form of BytesInput.
     * 
     * @return - byte array output stream packaged in BytesInput
     */
    public ListBytesInput getBytes() throws IOException {
        ListBytesInput resBytesInput;
        prepareEndWriteOnePage();
        BytesInput.PublicBAOS freqMetadata = new BytesInput.PublicBAOS();
        // first integer indicates whether it has frequency metadata
        if (freqEncoder != null) {
            ReadWriteStreamUtils.writeUnsignedVarInt(1, freqMetadata);
            ReadWriteStreamUtils.writeUnsignedVarInt(freout.size(), freqMetadata);
            resBytesInput = new ListBytesInput(freqMetadata, freout);
        } else {
            ReadWriteStreamUtils.writeUnsignedVarInt(0, freqMetadata);
            resBytesInput = new ListBytesInput(freqMetadata);
        }
        BytesInput.PublicBAOS timeMetadata = new BytesInput.PublicBAOS();
        ReadWriteStreamUtils.writeUnsignedVarInt(timeOut.size(), timeMetadata);
        resBytesInput.appendPublicBAOS(timeMetadata, timeOut, valueOut);
        return resBytesInput;
    }

    /**
     * calculate max possible memory size it occupies, including time outputStream, value outputStream and
     * frequency outputStream
     * 
     * @return allocated size in time, value and frequency encoder and outputStream
     */
    public long estimateMaxMemSize() {
        long size =  timeOut.size() + valueOut.size()+timeEncoder.getMaxByteSize()+valueEncoder.getMaxByteSize();
        if(freqEncoder != null)
            size += freout.size() + freqEncoder.getMaxByteSize();
        return size;
    }

    /**
     * reset data in ByteArrayOutputStream
     */
    public void reset() {
        timeOut = new BytesInput.PublicBAOS();
        valueOut = new BytesInput.PublicBAOS();
        if (freqEncoder != null)
            freout = new BytesInput.PublicBAOS();
    }

    public void setTimeEncoder(Encoder encoder) {
        this.timeEncoder = encoder;
    }

    public void setValueEncoder(Encoder encoder) {
        this.valueEncoder = encoder;
    }

    public void setFreqEncoder(Encoder freqEncoder) {
        this.freqEncoder = freqEncoder;
        if (freqEncoder != null)
            freout = new BytesInput.PublicBAOS();
    }
}
