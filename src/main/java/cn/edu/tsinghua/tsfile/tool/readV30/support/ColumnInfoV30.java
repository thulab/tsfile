package cn.edu.tsinghua.tsfile.tool.readV30.support;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

public class ColumnInfoV30 {
    private String name;
    private TSDataType dataType;

    public ColumnInfoV30(String name, TSDataType dataType) {
        this.setName(name);
        this.setDataType(dataType);
    }

    public TSDataType getDataType() {
        return dataType;
    }

    public void setDataType(TSDataType dataType) {
        this.dataType = dataType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String toString() {
        return getName() + ":" + getDataType();
    }

    public int hashCode() {
        return getName().hashCode();
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else {
            if (o instanceof cn.edu.tsinghua.tsfile.timeseries.read.support.ColumnInfo) {
                return this.getName().equals(((cn.edu.tsinghua.tsfile.timeseries.read.support.ColumnInfo) o).getName());
            } else {
                return false;
            }
        }
    }
}
