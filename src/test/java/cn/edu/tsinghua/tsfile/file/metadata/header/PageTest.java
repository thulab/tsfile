package cn.edu.tsinghua.tsfile.file.metadata.header;

import cn.edu.tsinghua.tsfile.file.header.PageHeader;
import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.file.metadata.utils.Utils;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class PageTest {

    private PageHeader pageHeader;

    @Test
    public void writeReadWithoutContentTest() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        pageHeader = TestHelper.createSimplePageHeader();
        ReadWriteToBytesUtils.write(pageHeader, outputStream);

        byte[] bytes = outputStream.toByteArray();

        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        PageHeader pageHeader2 = ReadWriteToBytesUtils.readPageHeader(inputStream);

        Utils.isPageHeaderEqual(pageHeader, pageHeader2);
    }

    @Test
    public void writeReadWithContentTest() throws IOException {
        String content = "This is the content.";

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        pageHeader = TestHelper.createSimplePageHeader();
        ReadWriteToBytesUtils.write(pageHeader, outputStream);
        ReadWriteToBytesUtils.write(content, outputStream);

        byte[] bytes = outputStream.toByteArray();

        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        PageHeader pageHeader2 = ReadWriteToBytesUtils.readPageHeader(inputStream);
        String content2 = ReadWriteToBytesUtils.readString(inputStream);

        Utils.isPageHeaderEqual(pageHeader, pageHeader2);
        assertEquals(content, content2);
    }
}
