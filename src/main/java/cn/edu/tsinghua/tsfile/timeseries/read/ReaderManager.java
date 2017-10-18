package cn.edu.tsinghua.tsfile.timeseries.read;

import cn.edu.tsinghua.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is used to construct {@code FileReader}. <br>
 * It is an adapter between {@code RecordReader} and {@code FileReader}
 *
 */
public class ReaderManager {

    private FileReader fileReader;
    private List<TSRandomAccessFileReader> rafList;
    private HashMap<String, List<RowGroupReader>> rowGroupReaderMap;
    private List<RowGroupReader> rowGroupReaderList;

    public ReaderManager(TSRandomAccessFileReader randomAccessFileReader) throws IOException {
        rowGroupReaderList = new ArrayList<>();
        rowGroupReaderMap = new HashMap<>();

        fileReader = new FileReader(randomAccessFileReader);
        addRowGroupReadersToMap(fileReader);
        addRowGroupReadersToList(fileReader);
    }

    private void addRowGroupReadersToMap(FileReader fileReader) {
        HashMap<String, ArrayList<RowGroupReader>> rgrMap = fileReader.getRowGroupReaderMap();
        for (String deltaObjectUID : rgrMap.keySet()) {
            if (rowGroupReaderMap.containsKey(deltaObjectUID)) {
                rowGroupReaderMap.get(deltaObjectUID).addAll(rgrMap.get(deltaObjectUID));
            } else {
                rowGroupReaderMap.put(deltaObjectUID, rgrMap.get(deltaObjectUID));
            }
        }
    }

    private void addRowGroupReadersToList(FileReader fileReader) {
        this.rowGroupReaderList.addAll(fileReader.getRowGroupReaderList());
    }

    public List<RowGroupReader> getAllRowGroupReaders() {
        return rowGroupReaderList;
    }

    public List<RowGroupReader> getRowGroupReaderListByDeltaObject(String deltaObjectUID) {
        List<RowGroupReader> ret = rowGroupReaderMap.get(deltaObjectUID);
        if (ret == null) {
            return new ArrayList<>();
        }
        return ret;
    }

    public TSDataType getDataTypeBySeriesName(String deltaObject, String measurement) {
        ArrayList<RowGroupReader> rgrList = fileReader.getRowGroupReaderMap().get(deltaObject);
        if (rgrList == null || rgrList.size() == 0) {
            return null;
        }
        return rgrList.get(0).getDataTypeBySeriesName(measurement);
    }

    public HashMap<String, List<RowGroupReader>> getRowGroupReaderMap() {
        return rowGroupReaderMap;
    }

    public Map<String, String> getProps() {
        return fileReader.getProps();
    }

    public String getProp(String key) {
        return fileReader.getProp(key);
    }

    /**
     *
     * @throws IOException exception in IO
     */
    public void close() throws IOException {
        for (TSRandomAccessFileReader raf : rafList) {
            if (raf instanceof LocalFileInput) {
                ((LocalFileInput) raf).closeFromManager();
            } else {
                raf.close();
            }
        }
    }
}
