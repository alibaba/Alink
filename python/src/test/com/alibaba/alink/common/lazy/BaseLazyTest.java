package com.alibaba.alink.common.lazy;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import com.alibaba.alink.common.MLEnvironment;
import com.alibaba.alink.common.MLEnvironmentFactory;

import org.apache.commons.io.output.TeeOutputStream;
import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Before;

public abstract class BaseLazyTest {

    protected static final Row[] TRAIN_ARRAY_DATA = new Row[] {
        Row.of("A", 1L, 1L, 0.6),
        Row.of("A", 2L, 2L, 0.8),
        Row.of("A", 2L, 3L, 0.6),
        Row.of("A", 3L, 1L, 0.6),
        Row.of("B", 3L, 2L, 0.3),
        Row.of("B", 3L, 3L, 0.4),
    };

    protected ByteArrayOutputStream outContent;
    protected PrintStream systemOut;
    protected long mlEnvId;

    @Before
    public void before() {
        systemOut = System.out;
        outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(new TeeOutputStream(systemOut, outContent)));

        MLEnvironmentFactory.remove(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID);
        MLEnvironmentFactory.setDefault(new MLEnvironment());
    }

    @After
    public void after() {
        System.setOut(systemOut);
        MLEnvironmentFactory.remove(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID);
    }
}
