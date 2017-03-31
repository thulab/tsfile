package com.corp.delta.tsfile.write.series;

import java.io.IOException;
import java.math.BigDecimal;

import com.corp.delta.tsfile.common.utils.Binary;
import com.corp.delta.tsfile.common.utils.ReadWriteStreamUtils;
import com.corp.delta.tsfile.common.utils.bytesinput.BytesInput.PublicBAOS;
import com.corp.delta.tsfile.common.utils.bytesinput.ListBytesInput;
import com.corp.delta.tsfile.encoding.encoder.Encoder;

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
    private PublicBAOS timeOut;
    // value
    private Encoder valueEncoder;
    private PublicBAOS valueOut;

    // frequency
    private Encoder freqEncoder = null;
    private PublicBAOS freout;

    public ValueWriter() {
        this.timeOut = new PublicBAOS();
        this.valueOut = new PublicBAOS();
        this.freout = new PublicBAOS();
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
        PublicBAOS freqMetadata = new PublicBAOS();
        // first integer indicates whether it has frequency metadata
        if (freqEncoder != null) {
            ReadWriteStreamUtils.writeUnsignedVarInt(1, freqMetadata);
            ReadWriteStreamUtils.writeUnsignedVarInt(freout.size(), freqMetadata);
            resBytesInput = new ListBytesInput(freqMetadata, freout);
        } else {
            ReadWriteStreamUtils.writeUnsignedVarInt(0, freqMetadata);
            resBytesInput = new ListBytesInput(freqMetadata);
        }
        PublicBAOS timeMetadata = new PublicBAOS();
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
        timeOut = new PublicBAOS();
        valueOut = new PublicBAOS();
        if (freqEncoder != null)
            freout = new PublicBAOS();
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
            freout = new PublicBAOS();
    }
}
