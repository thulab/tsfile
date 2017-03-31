package com.corp.delta.tsfile.utils;

import static org.junit.Assert.*;

import org.junit.Test;

import com.corp.delta.tsfile.constant.TimeseriesTestConstant;
import com.corp.delta.tsfile.utils.FileUtils;
import com.corp.delta.tsfile.utils.FileUtils.Unit;

import java.util.Random;

/**
 * 
 * @author kangrong
 *
 */
public class FileUtilsTest {

    @Test
    public void testConvertUnit() {
        long kb = 3 * 1024;
        long mb = kb * 1024;
        long gb = mb * 1024;
        assertEquals(3.0 * 1024, FileUtils.transformUnit(kb, Unit.B),
                TimeseriesTestConstant.double_min_delta);
        assertEquals(3, FileUtils.transformUnit(kb, Unit.KB),
                TimeseriesTestConstant.double_min_delta);

        assertEquals(3, FileUtils.transformUnit(mb, Unit.MB),
                TimeseriesTestConstant.double_min_delta);
        assertEquals(3, FileUtils.transformUnit(gb, Unit.GB),
                TimeseriesTestConstant.double_min_delta);
    }

    @Test
    public void testConvertToByte() {
        assertEquals(3l, (long) FileUtils.transformUnitToByte(3, Unit.B));
        assertEquals(3l * 1024, (long) FileUtils.transformUnitToByte(3, Unit.KB));
        assertEquals(3l * 1024 * 1024, (long) FileUtils.transformUnitToByte(3, Unit.MB));
        assertEquals(3l * 1024 * 1024 * 1024, (long) FileUtils.transformUnitToByte(3, Unit.GB));
    }

    @Test
    public void testGetLocalFileByte() {
        String fileName = "src/test/resources/testFileBlock.yaml";
        assertEquals(523.0, FileUtils.getLocalFileByte(fileName, Unit.B),
                TimeseriesTestConstant.double_min_delta);
        assertEquals(0.51, FileUtils.getLocalFileByte(fileName, Unit.KB),
                TimeseriesTestConstant.double_min_delta);
    }

    @Test
    public void test() {
        Random r = new Random();
        char ch1 = (char) (97+r.nextInt(25));
        System.out.println("ch1 = " + ch1);  // 将char类型数字8转换为int类型数字8
        // 方法一：
        Character ch2 = '8'; // char是基本数据类型，Character是其包装类型。
        int num2 = Integer.parseInt(ch2.toString());
        System.out.println("num2 = " + num2);
        // 方法二：
        char ch3 = '8';
        int num3 = ch3 - 48;
        System.out.println("num3 = " + num3);
    }
}
