package cn.edu.tsinghua.tsfile.encoding.encoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;

public class DoublePrecisionEncoder extends GorillaEncoder {
	private long preValue;

	public DoublePrecisionEncoder() {
	}

	@Override
	public void encode(double value, ByteArrayOutputStream out) throws IOException {
		if (!flag) {
			flag = true;
			preValue = Double.doubleToLongBits(value);
			leadingZeroNum = Long.numberOfLeadingZeros(preValue);
			tailingZeroNum = Long.numberOfTrailingZeros(preValue);
			byte[] bufferBig = new byte[8];
			byte[] bufferLittle = new byte[8];

			for (int i = 0; i < 8; i++) {
				bufferLittle[i] = (byte) (((preValue) >> (i * 8)) & 0xFF);
				bufferBig[8 - i - 1] = (byte) (((preValue) >> (i * 8)) & 0xFF);
			}
			out.write(bufferLittle);
		} else {
			long nextValue = Double.doubleToLongBits(value);
			long tmp = nextValue ^ preValue;
			if (tmp == 0) {
				writeBit(false, out);
				writeBit(false, out);
			} else {
				int leadingZeroNumTmp = Long.numberOfLeadingZeros(tmp);
				int tailingZeroNumTmp = Long.numberOfTrailingZeros(tmp);
				if (leadingZeroNumTmp >= leadingZeroNum && tailingZeroNumTmp >= tailingZeroNum) {
					writeBit(true, out);
					writeBit(false, out);
					writeBits(tmp, out, TSFileConfig.DOUBLE_LENGTH - 1 - leadingZeroNum, tailingZeroNum);
				} else {
					writeBit(true, out);
					writeBit(true, out);
					writeBits(leadingZeroNumTmp, out, TSFileConfig.DOUBLE_LEADING_ZERO_LENGTH - 1, 0);
					writeBits(32 - leadingZeroNumTmp - tailingZeroNumTmp, out, TSFileConfig.DOUBLE_VALUE_LENGTH - 1, 0);
					writeBits(tmp, out, TSFileConfig.DOUBLE_LENGTH - 1 - leadingZeroNumTmp, tailingZeroNumTmp);
				}
			}
		}
	}

	private void writeBits(long num, ByteArrayOutputStream out, int start, int end) {
		for (int i = start; i >= end; i--) {
			long bit = num & (1L << i);
			writeBit(bit, out);
		}
	}
}
