package cn.edu.tsinghua.tsfile.encoding.encoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;

public abstract class FloatEncoder2 extends Encoder{
	protected boolean flag;
	protected int leadingZeroNum, tailingZeroNum;
	// 8-bit buffer of bits to write out
	protected byte buffer;
	// number of bits remaining in buffer
	protected int n;
	
	public FloatEncoder2(TSEncoding type) {
		super(type);
		this.flag = false;
	}

	@Override
	public void flush(ByteArrayOutputStream out) throws IOException {
		// TODO Auto-generated method stub
		writeBit(false, out);
		writeBit(true, out);
		clearBuffer(out);
		reset();
	}

	protected void writeBit(boolean b, ByteArrayOutputStream out){
		// add bit to buffer
        buffer <<= 1;
        if (b) buffer |= 1;

        // if buffer is full (8 bits), write out as a single byte
        n++;
        if (n == 8) clearBuffer(out);
	}
	
	protected void writeBit(int i, ByteArrayOutputStream out){
		if(i == 0){
			writeBit(false, out);
		} else{
			writeBit(true, out);
		}
	}
	
	protected void writeBit(long i, ByteArrayOutputStream out){
		if(i == 0){
			writeBit(false, out);
		} else{
			writeBit(true, out);
		}
	}
	
	protected void clearBuffer(ByteArrayOutputStream out){
		if (n == 0) return;
        if (n > 0) buffer <<= (8 - n);
        out.write(buffer);
        n = 0;
        buffer = 0;
	}
	
	private void reset(){
		this.flag = false;
		this.n = 0;
		this.buffer = 0;
	}
}
