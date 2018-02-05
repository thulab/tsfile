package cn.edu.tsinghua.tsfile.file.header;

import cn.edu.tsinghua.tsfile.file.IBytesConverter;

import java.io.InputStream;
import java.io.OutputStream;

public class IndexPageHeader implements IBytesConverter {

    public IndexPageHeader() {
    }

    public int write(OutputStream outputStream){
        return 0;
    }

    public void read(InputStream inputStream){

    }
}
