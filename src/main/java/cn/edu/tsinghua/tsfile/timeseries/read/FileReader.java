package cn.edu.tsinghua.tsfile.timeseries.read;

import cn.edu.tsinghua.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TSFileMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.converter.TSFileMetaDataConverter;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TSFileIOWriter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is used to read <code>TSFileMetaData</code>} and construct
 * file level reader which contains the information of <code>RowGroupReader</code>.
 *
 * @author Jinrui Zhang
 */
public class FileReader {
    private static final int FOOTER_LENGTH = 4;
    private static final int MAGIC_LENGTH = TSFileIOWriter.magicStringBytes.length;
    /**
     * If the file has many rowgroups and series,
     * the storage of <code>fileMetaData</code> may be large.
     */
    private TSFileMetaData fileMetaData;
    private TSRandomAccessFileReader randomAccessFileReader;
    /**
     * TODO Are rowGroupReaderList and rowGroupReaderMap all needed?
     */
    private ArrayList<RowGroupReader> rowGroupReaderList;
    private HashMap<String, ArrayList<RowGroupReader>> rowGroupReaderMap;

    public FileReader(TSRandomAccessFileReader randomAccessFileReader) throws IOException {
        this.randomAccessFileReader = randomAccessFileReader;
        init();
    }

    /**
     * <code>FileReader</code> initialization, construct <code>fileMetaData</code>
     * <code>rowGroupReaderList</code>, and <code>rowGroupReaderMap</code>.
     *
     * @throws IOException file read error
     */
    private void init() throws IOException {
        long l = randomAccessFileReader.length();
        randomAccessFileReader.seek(l - MAGIC_LENGTH - FOOTER_LENGTH);
        int fileMetaDataLength = randomAccessFileReader.readInt();
        randomAccessFileReader.seek(l - MAGIC_LENGTH - FOOTER_LENGTH - fileMetaDataLength);
        byte[] buf = new byte[fileMetaDataLength];
        randomAccessFileReader.read(buf, 0, buf.length);

        ByteArrayInputStream bais = new ByteArrayInputStream(buf);
        this.fileMetaData = new TSFileMetaDataConverter().toTSFileMetadata(ReadWriteThriftFormatUtils.readFileMetaData(bais));

        rowGroupReaderList = new ArrayList<>();
        rowGroupReaderMap = new HashMap<>();
        initFromRowGroupMetadataList(fileMetaData.getRowGroups());
    }

    private void initFromRowGroupMetadataList(List<RowGroupMetaData> rowGroupMetadataList) {
        rowGroupReaderList = new ArrayList<>();
        rowGroupReaderMap = new HashMap<>();
        for (RowGroupMetaData rowGroupMetaData : rowGroupMetadataList) {
            String key = rowGroupMetaData.getDeltaObjectUID();
            RowGroupReader rowGroupReader = new RowGroupReader(rowGroupMetaData, randomAccessFileReader);
            rowGroupReaderList.add(rowGroupReader);
            if (!rowGroupReaderMap.containsKey(key)) {
                ArrayList<RowGroupReader> rowGroupReaderList = new ArrayList<>();
                rowGroupReaderList.add(rowGroupReader);
                rowGroupReaderMap.put(key, rowGroupReaderList);
            } else {
                rowGroupReaderMap.get(key).add(rowGroupReader);
            }
        }
    }

    public HashMap<String, ArrayList<RowGroupReader>> getRowGroupReaderMap() {
        return this.rowGroupReaderMap;
    }

    public ArrayList<RowGroupReader> getRowGroupReaderList() {
        return this.rowGroupReaderList;
    }

    /**
     * @param deltaObjectUID delta object id
     * @param index          from 0 to n-1
     * @return reader
     */
    public RowGroupReader getRowGroupReader(String deltaObjectUID, int index) {
        return this.rowGroupReaderMap.get(deltaObjectUID).get(index);
    }

    public Map<String, String> getProps() {
        return fileMetaData.getProps();
    }

    public String getProp(String key) {
        return fileMetaData.getProp(key);
    }

    public void close() throws IOException {
        this.randomAccessFileReader.close();
    }
}
