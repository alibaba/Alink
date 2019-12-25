package com.alibaba.alink.operator.common.io.csv;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases for {@link CsvFileInputSplit}.
 */
public class CsvFileInputSplitTest {
    @Test
    public void testLargeFile() throws Exception {
        CsvFileInputSplit split1 = new CsvFileInputSplit(2, 0, (long) Integer.MAX_VALUE * 4L + 1L);
        CsvFileInputSplit split2 = new CsvFileInputSplit(2, 1, (long) Integer.MAX_VALUE * 4L + 1L);
        Assert.assertEquals(split1.start, 0L);
        Assert.assertEquals(split1.length, (long) Integer.MAX_VALUE * 2 + 1L);
        Assert.assertEquals(split1.end, (long) Integer.MAX_VALUE * 2 + 1L + 1024L * 1024L);
        Assert.assertEquals(split2.start, (long) Integer.MAX_VALUE * 2 + 1L);
        Assert.assertEquals(split2.length, (long) Integer.MAX_VALUE * 2);
        Assert.assertEquals(split2.end, (long) Integer.MAX_VALUE * 4L + 1L);
    }

    @Test
    public void testCornerCase() throws Exception {
        CsvFileInputSplit split1 = new CsvFileInputSplit(2, 0, 1L);
        CsvFileInputSplit split2 = new CsvFileInputSplit(2, 1, 1L);
        Assert.assertEquals(split1.start, 0L);
        Assert.assertEquals(split1.length, 1L);
        Assert.assertEquals(split1.end, 1L);
        Assert.assertEquals(split2.start, 1L);
        Assert.assertEquals(split2.length, 0L);
        Assert.assertEquals(split2.end, 1L);
    }
}