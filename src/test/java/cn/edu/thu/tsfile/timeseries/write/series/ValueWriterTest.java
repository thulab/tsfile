package cn.edu.thu.tsfile.timeseries.write.series;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import cn.edu.thu.tsfile.common.utils.ReadWriteStreamUtils;
import cn.edu.thu.tsfile.timeseries.constant.TimeseriesTestConstant;
import cn.edu.thu.tsfile.encoding.common.EndianType;
import cn.edu.thu.tsfile.encoding.decoder.PlainDecoder;
import cn.edu.thu.tsfile.encoding.encoder.PlainEncoder;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

import cn.edu.thu.tsfile.common.utils.Binary;
import cn.edu.thu.tsfile.common.utils.bytesinput.BytesInput;

/**
 * 
 * @author kangrong
 *
 */
public class ValueWriterTest {

    @Test
    public void testNoFreq() {
        ValueWriter writer = new ValueWriter();
        writer.setTimeEncoder(new PlainEncoder(EndianType.LITTLE_ENDIAN, TSDataType.INT64, 0));
        writer.setValueEncoder(new PlainEncoder(EndianType.LITTLE_ENDIAN, TSDataType.INT64, 0));
        short s1 = 12;
        boolean b1 = false;
        int i1 = 1;
        long l1 = 123142120391l;
        float f1 = 2.2f;
        double d1 = 1294283.4323d;
        // BigDecimal big1 = new BigDecimal("123.43");
        String str1 = "I have a dream";
        int timeCount = 0;
        try {
            writer.write(timeCount++, s1);
            writer.write(timeCount++, b1);
            writer.write(timeCount++, i1);
            writer.write(timeCount++, l1);
            writer.write(timeCount++, f1);
            writer.write(timeCount++, d1);
            // writer.write(timeCount++, big1);
            writer.write(timeCount++, new Binary(str1));
            assertEquals(101, writer.estimateMaxMemSize());
            BytesInput input = writer.getBytes();
            writer.reset();
            assertEquals(0, writer.estimateMaxMemSize());
            ByteArrayInputStream in = new ByteArrayInputStream(input.toByteArray());
            int timeSize = ReadWriteStreamUtils.readUnsignedVarInt(in);
            System.out.println(timeSize);
//            assertEquals(timeCount * 8, timeSize);
            byte[] timeBytes = new byte[timeSize];
            in.read(timeBytes);
            ByteArrayInputStream timeInputStream = new ByteArrayInputStream(timeBytes);
            PlainDecoder decoder = new PlainDecoder(EndianType.LITTLE_ENDIAN);
            for (int i = 0; i < timeCount; i++) {
                assertEquals(i, decoder.readLong(timeInputStream));
            }
            assertEquals(s1, decoder.readShort(in));
            assertEquals(b1, decoder.readBoolean(in));
            assertEquals(i1, decoder.readInt(in));
            assertEquals(l1, decoder.readLong(in));
            Assert.assertEquals(f1, decoder.readFloat(in), TimeseriesTestConstant.float_min_delta);
            assertEquals(d1, decoder.readDouble(in), TimeseriesTestConstant.double_min_delta);
            assertEquals(str1, decoder.readBinary(in).getStringValue());

        } catch (IOException e) {
            fail();
        }
    }
}
