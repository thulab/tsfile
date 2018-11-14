package cn.edu.tsinghua.tsfile.file;

/**
 * MetaMarker denotes the type of headers and footers. Enum is not used for space saving.
 */
public class MetaMarker {
    public static final byte RowGroupFooter = 0;
    public static final byte ChunkHeader = RowGroupFooter + 1;
}
