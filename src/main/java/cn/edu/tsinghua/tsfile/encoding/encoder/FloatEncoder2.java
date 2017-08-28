package cn.edu.tsinghua.tsfile.encoding.encoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;

public class FloatEncoder2 extends Encoder{
	private boolean flag;
	private int preValue;
	private int leadingZeroNum, tailingZeroNum;
	// 8-bit buffer of bits to write out
	private byte buffer;
	// number of bits remaining in buffer
	private int n;
	
	public FloatEncoder2(TSEncoding type) {
		super(type);
		this.flag = false;
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
        			int leadingZeroNumTmp = Integer.numberOfLeadingZeros(nextValue);
            		int tailingZeroNumTmp = Integer.numberOfTrailingZeros(nextValue);
            		if(leadingZeroNumTmp >= leadingZeroNum && tailingZeroNumTmp >= tailingZeroNum){
            			writeBit(true, out);
            			writeBit(false, out);
            			writeBits(nextValue, out, 32 - 1 - leadingZeroNum, tailingZeroNum);     
            		} else{
            			writeBit(true, out);
            			writeBit(true, out);
            			writeBits(leadingZeroNumTmp, out, 4, 0);
            			writeBits(32 - leadingZeroNumTmp - tailingZeroNumTmp, out, 4, 0);
            			writeBits(nextValue, out, 32 - 1 - leadingZeroNumTmp, tailingZeroNumTmp); 
            		}
        		}
        }
    }
    
	@Override
	public void flush(ByteArrayOutputStream out) throws IOException {
		// TODO Auto-generated method stub
		writeBit(false, out);
		writeBit(true, out);
		clearBuffer(out);
	}

	private void writeBit(boolean b, ByteArrayOutputStream out){
		// add bit to buffer
        buffer <<= 1;
        if (b) buffer |= 1;

        // if buffer is full (8 bits), write out as a single byte
        n++;
        if (n == 8) clearBuffer(out);
	}
	
	private void writeBit(int i, ByteArrayOutputStream out){
		if(i == 0){
			writeBit(false, out);
		} else{
			writeBit(true, out);
		}
	}
	
	private void writeBits(int num, ByteArrayOutputStream out, int start, int end){
		for(int i = start; i >= end; i--){
			int bit = num & (1 << i);
			writeBit(bit, out);
		}
	}
	
	private void clearBuffer(ByteArrayOutputStream out){
		if (n == 0) return;
        if (n > 0) buffer <<= (8 - n);
        out.write(buffer);
        n = 0;
        buffer = 0;
	}
}
