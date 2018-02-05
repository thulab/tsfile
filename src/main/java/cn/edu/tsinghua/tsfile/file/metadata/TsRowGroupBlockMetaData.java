package cn.edu.tsinghua.tsfile.file.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.tsfile.file.IBytesConverter;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;

public class TsRowGroupBlockMetaData implements IBytesConverter {
	/**
     * Row groups in this file
     */
    private List<RowGroupMetaData> rowGroupMetadataList;
    
    private String deltaObjectID;
    
    public TsRowGroupBlockMetaData(){
    		rowGroupMetadataList = new ArrayList<>();
    }
    
    public TsRowGroupBlockMetaData(List<RowGroupMetaData> rowGroupMetadataList){
    		this.rowGroupMetadataList = rowGroupMetadataList;
    }
    
    /**
     * add row group metadata to rowGroups. THREAD NOT SAFE
     * @param rowGroup - row group metadata to add
     */
    public void addRowGroupMetaData(RowGroupMetaData rowGroup) {
        if (rowGroupMetadataList == null) {
            rowGroupMetadataList = new ArrayList<RowGroupMetaData>();
        }
        rowGroupMetadataList.add(rowGroup);
    }
    
    public List<RowGroupMetaData> getRowGroups() {
        return rowGroupMetadataList;
    }

    public void setRowGroups(List<RowGroupMetaData> rowGroupMetadataList) {
        this.rowGroupMetadataList = rowGroupMetadataList;
    }

    public int write(OutputStream outputStream) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteToBytesUtils.writeIsNull(rowGroupMetadataList, outputStream);
        if(rowGroupMetadataList != null){
            byteLen += ReadWriteToBytesUtils.write(rowGroupMetadataList.size(), outputStream);

            for(RowGroupMetaData rowGroupMetaData : rowGroupMetadataList)
                byteLen += ReadWriteToBytesUtils.write(rowGroupMetaData, outputStream);
        }

        byteLen += ReadWriteToBytesUtils.writeIsNull(deltaObjectID, outputStream);
        if(deltaObjectID != null)byteLen += ReadWriteToBytesUtils.write(deltaObjectID, outputStream);

        return byteLen;
    }

    public void read(InputStream inputStream) throws IOException {

        if(ReadWriteToBytesUtils.readIsNull(inputStream)){
            rowGroupMetadataList = new ArrayList<>();

            int size = ReadWriteToBytesUtils.readInt(inputStream);
            for(int i = 0;i < size;i++)
                rowGroupMetadataList.add(ReadWriteToBytesUtils.readRowGroupMetaData(inputStream));
        }

        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            deltaObjectID = ReadWriteToBytesUtils.readString(inputStream);
    }

	public String getDeltaObjectID() {
		return deltaObjectID;
	}

	public void setDeltaObjectID(String deltaObjectID) {
		this.deltaObjectID = deltaObjectID;
	}
}
