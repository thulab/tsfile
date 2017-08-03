package cn.edu.thu.tsfile.timeseries.read;

import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.thu.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.thu.tsfile.file.metadata.TSFileMetaData;
import cn.edu.thu.tsfile.file.metadata.converter.TSFileMetaDataConverter;
import cn.edu.thu.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.thu.tsfile.timeseries.write.io.TSFileIOWriter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Jinrui Zhang
 * This class is used to read {@code TSFileMetaData} and construct
 * file level reader which contains the information of
 * rowgroupreader.
 */
public class FileReader {
    public static final int FOOTER_LENGTH = 4;
    public static final int MAGIC_LENGTH = TSFileIOWriter.magicStringBytes.length;
    private TSFileMetaData fileMetaData;
    private ByteArrayInputStream bais;
    private TSRandomAccessFileReader raf;
    private ArrayList<RowGroupReader> rowGroupReaderList;
    private HashMap<String, ArrayList<RowGroupReader>> rowGroupReadersMap;

    public FileReader(TSRandomAccessFileReader raf) throws IOException {
        this.raf = raf;
        init();
    }

    public FileReader(TSRandomAccessFileReader raf, List<RowGroupMetaData> rowGroupMetaDataList) {
        this.raf = raf;
        initFromRowGroupMetadataList(rowGroupMetaDataList);
    }

    /**
     * FileReader initialize, constructing fileMetaData and rowGroupReaders
     *
     * @throws IOException cannot init
     */
    private void init() throws IOException {
        long l = raf.length();
        raf.seek(l - MAGIC_LENGTH - FOOTER_LENGTH);
        int fileMetaDataLength = raf.readInt();

        raf.seek(l - MAGIC_LENGTH - FOOTER_LENGTH - fileMetaDataLength);
        byte[] buf = new byte[fileMetaDataLength];
        raf.read(buf, 0, buf.length);
        bais = new ByteArrayInputStream(buf);

        this.fileMetaData = new TSFileMetaDataConverter()
                .toTSFileMetadata(ReadWriteThriftFormatUtils.readFileMetaData(bais));

        rowGroupReaderList = new ArrayList<>();
        rowGroupReadersMap = new HashMap<>();
        initFromRowGroupMetadataList(fileMetaData.getRowGroups());
    }

    private void initFromRowGroupMetadataList(List<RowGroupMetaData> rowGroupMetadataList) {
        rowGroupReaderList = new ArrayList<>();
        rowGroupReadersMap = new HashMap<>();
        for (RowGroupMetaData rowGroupMetaData : rowGroupMetadataList) {
            String key = rowGroupMetaData.getDeltaObjectUID();
            RowGroupReader rowGroupReader = new RowGroupReader(rowGroupMetaData, raf);
            rowGroupReaderList.add(rowGroupReader);
            if (!rowGroupReadersMap.containsKey(key)) {
                ArrayList<RowGroupReader> rowGroupReaderList = new ArrayList<>();
                rowGroupReaderList.add(rowGroupReader);
                rowGroupReadersMap.put(key, rowGroupReaderList);
            } else {
                rowGroupReadersMap.get(key).add(rowGroupReader);
            }
        }
    }

    public ArrayList<RowGroupReader> getOneRowGroupReader(String deltaObjectUID) {
        return this.rowGroupReadersMap.get(deltaObjectUID);
    }

    public HashMap<String, ArrayList<RowGroupReader>> getRowGroupReadersMap() {
        return this.rowGroupReadersMap;
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
        return this.rowGroupReadersMap.get(deltaObjectUID).get(index);
    }

    public Map<String, String> getProps() {
        return fileMetaData.getProps();
    }

    public String getProp(String key) {
        return fileMetaData.getProp(key);
    }

    /**
     * @return the footer of the file
     */
    public TSFileMetaData getFileMetadata() {
        return this.fileMetaData;
    }

    public void close() throws IOException {
        this.raf.close();
    }
}
