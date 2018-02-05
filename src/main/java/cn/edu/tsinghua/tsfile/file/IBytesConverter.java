package cn.edu.tsinghua.tsfile.file;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface IBytesConverter {

    public int write(OutputStream outputStream) throws IOException;
    public void read(InputStream inputStream) throws IOException;
}
