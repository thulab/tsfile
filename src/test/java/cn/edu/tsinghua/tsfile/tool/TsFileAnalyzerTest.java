package cn.edu.tsinghua.tsfile.tool;

import org.junit.Test;

import java.io.IOException;

public class TsFileAnalyzerTest {
    
    public static void main(String[] args) throws IOException {
        String inputFilePath = "test.ts";
        String outputFilePath = "report.txt";
        TsFileAnalyzer analyzer = new TsFileAnalyzer();
        analyzer.analyze(inputFilePath);
        analyzer.output(outputFilePath);
    }
}
