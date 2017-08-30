package cn.edu.tsinghua.tsfile.encoding.encoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;

public class SinglePrecisionEncoder extends GorillaEncoder{
	private int preValue;

	public SinglePrecisionEncoder() {	
	}
	
    @Override
    public void encode(float value, ByteArrayOutputStream out) throws IOException {
        if(!flag){
        		flag = true;
        		preValue = Float.floatToIntBits(value);
        		leadingZeroNum = Integer.numberOfLeadingZeros(preValue);
        		tailingZeroNum = Integer.numberOfTrailingZeros(preValue);
        		out.write((preValue >> 0) & 0xFF);
        		out.write((preValue >> 8) & 0xFF);
        		out.write((preValue >> 16) & 0xFF);
        		out.write((preValue >> 24) & 0xFF);
        } else{
        		int nextValue = Float.floatToIntBits(value);
        		int tmp = nextValue ^ preValue;
        		if(tmp == 0){
        			writeBit(false, out);
        			writeBit(false, out);
        		} else{
        			int leadingZeroNumTmp = Integer.numberOfLeadingZeros(tmp);
            		int tailingZeroNumTmp = Integer.numberOfTrailingZeros(tmp);
            		if(leadingZeroNumTmp >= leadingZeroNum && tailingZeroNumTmp >= tailingZeroNum){
            			writeBit(true, out);
            			writeBit(false, out);
            			writeBits(tmp, out, TSFileConfig.FLOAT_LENGTH - 1 - leadingZeroNum, tailingZeroNum);     
            		} else{
            			writeBit(true, out);
            			writeBit(true, out);
            			writeBits(leadingZeroNumTmp, out, TSFileConfig.FLAOT_LEADING_ZERO_LENGTH - 1, 0);
            			writeBits(32 - leadingZeroNumTmp - tailingZeroNumTmp, out, TSFileConfig.FLOAT_VALUE_LENGTH - 1, 0);
            			writeBits(tmp, out, TSFileConfig.FLOAT_LENGTH - 1 - leadingZeroNumTmp, tailingZeroNumTmp); 
            		}
        		}
        		preValue = nextValue;
        }
    }
    
	private void writeBits(int num, ByteArrayOutputStream out, int start, int end){
		for(int i = start; i >= end; i--){
			int bit = num & (1 << i);
			writeBit(bit, out);
		}
	}
	
    @Override
    public int getOneItemMaxSize() {
        return 4;
    }

    @Override
    public long getMaxByteSize() {
        return 4 + 14;
    }
}
