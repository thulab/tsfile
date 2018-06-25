package cn.edu.tsinghua.tsfile.file.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * @author East
 */
public interface ITsSerializable {
    int serialize(OutputStream outputStream) throws IOException;
    int serialize(ByteBuffer buffer) throws IOException;
}
