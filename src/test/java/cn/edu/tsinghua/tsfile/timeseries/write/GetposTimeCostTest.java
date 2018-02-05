package cn.edu.tsinghua.tsfile.timeseries.write;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class GetposTimeCostTest {

    private String path = "src/test/resources/timecost.ts";
    private RandomAccessFile raf;

    @Before
    public void init() throws FileNotFoundException {
        File file = new File(path);
        if(file.exists())file.delete();

        raf = new RandomAccessFile(file, "rw");
    }

    @After
    public void after() throws IOException {
        raf.close();

        File file = new File(path);
        if(file.exists())file.delete();
    }

    @Test
    public void timeCostTest() throws IOException {
        int looptime = 1000000;
        long startTime, endTime;

        raf.write(new byte[100]);

        startTime = System.currentTimeMillis();
        for(int i = 0;i < looptime;i++)
            raf.length();
        endTime = System.currentTimeMillis();
        System.out.println("raf.length():" + (endTime - startTime));

        startTime = System.currentTimeMillis();
        for(int i = 0;i < looptime;i++)
            ;
        endTime = System.currentTimeMillis();
        System.out.println("do nothing:" + (endTime - startTime));
    }
}
