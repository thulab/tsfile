package cn.edu.tsinghua.tsfile.timeseries.read;

import cn.edu.tsinghua.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TSFileMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.converter.TSFileMetaDataConverter;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
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
     * TODO rowGroupReaderList could be removed.
     */
    private List<RowGroupReader> rowGroupReaderList;
    private Map<String, List<RowGroupReader>> rowGroupReaderMap;

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

    public Map<String, List<RowGroupReader>> getRowGroupReaderMap() {
        return this.rowGroupReaderMap;
    }

    public List<RowGroupReader> getRowGroupReaderList() {
        return this.rowGroupReaderList;
    }

    public Map<String, String> getProps() {
        return fileMetaData.getProps();
    }

    public String getProp(String key) {
        return fileMetaData.getProp(key);
    }

    public List<RowGroupReader> getRowGroupReaderListByDeltaObject(String deltaObjectUID) {
        List<RowGroupReader> ret = rowGroupReaderMap.get(deltaObjectUID);
        if (ret == null) {
            return new ArrayList<>();
        }
        return ret;
    }

    public TSDataType getDataTypeBySeriesName(String deltaObject, String measurement) {
        List<RowGroupReader> rgrList = getRowGroupReaderMap().get(deltaObject);
        if (rgrList == null || rgrList.size() == 0) {
            return null;
        }
        return rgrList.get(0).getDataTypeBySeriesName(measurement);
    }

    public void close() throws IOException {
        this.randomAccessFileReader.close();
    }
}
