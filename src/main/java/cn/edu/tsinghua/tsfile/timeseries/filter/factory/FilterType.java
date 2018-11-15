package cn.edu.tsinghua.tsfile.timeseries.filter.factory;

/**
 * Created by zhangjinrui on 2017/12/15.
 */
public enum FilterType {
    VALUE_FILTER("value"), TIME_FILTER("time");

    private String name;
    FilterType(String name){
        this.name = name;
    }

    public String toString(){
        return name;
    }

}
