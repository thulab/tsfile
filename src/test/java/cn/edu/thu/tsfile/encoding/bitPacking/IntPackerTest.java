package cn.edu.thu.tsfile.encoding.bitPacking;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Random;

import org.junit.Test;

/**
 * 
 * @author Zhang Jinrui
 *
 */
public class IntPackerTest {

	@Test
	public void test() {
		Random rand = new Random();
		int width = 31;

		int count = 100000;
		ArrayList<Integer> preValues = new ArrayList<Integer>();
		IntPacker packer = new IntPacker(width);
		byte[] bb = new byte[count * width];
		int idx = 0;
		for (int i = 0; i < count; i++) {
			int[] vs = new int[8];
			for(int j = 0 ; j < 8 ; j++){
				vs[j] = rand.nextInt(Integer.MAX_VALUE);
				preValues.add(vs[j]);
			}
			byte[] tb = new byte[width];
			packer.pack8Values(vs, 0, tb);
			for (int j = 0; j < tb.length; j++) {
				bb[idx++] = tb[j];
			}
		}
		int res[] = new int[count * 8];
		packer.unpackAllValues(bb, 0, bb.length, res);
		
		for(int i = 0 ; i < count * 8 ; i ++){
			int v = preValues.get(i);
			assertEquals(res[i], v);
		}
	}
}
