package com.alibaba.alink.operator.common.nlp;

import com.alibaba.alink.params.nlp.StopWordsRemoverParams;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for StopWordsRemoverMapper.
 */
public class StopWordsRemoverMapperTest {
    @Test
    public void testStopWords() throws Exception {
        TableSchema schema = new TableSchema(new String[] {"sentence"}, new TypeInformation<?>[] {Types.STRING});

        Params params = new Params()
            .set(StopWordsRemoverParams.SELECTED_COL, "sentence")
            .set(StopWordsRemoverParams.STOP_WORDS, new String[]{"Test"});

        StopWordsRemoverMapper mapper = new StopWordsRemoverMapper(schema, params);

        assertEquals(mapper.map(Row.of("This is a unit test for filtering stopWords")).getField(0),
            "unit filtering stopWords");
        assertEquals(mapper.map(Row.of("Filter stopWords test")).getField(0),
            "Filter stopWords");
        assertEquals(mapper.map(Row.of("这 是 停用词 过滤 的 单元 测试")).getField(0), "停用词 过滤 单元 测试");
        assertEquals(mapper.getOutputSchema(), schema);
    }

    @Test
    public void testCaseSensitive() throws Exception {
        TableSchema schema = new TableSchema(new String[] {"sentence"}, new TypeInformation<?>[] {Types.STRING});

        Params params = new Params()
            .set(StopWordsRemoverParams.SELECTED_COL, "sentence")
            .set(StopWordsRemoverParams.CASE_SENSITIVE, true)
            .set(StopWordsRemoverParams.STOP_WORDS, new String[]{"Test"});

        StopWordsRemoverMapper mapper = new StopWordsRemoverMapper(schema, params);

        assertEquals(mapper.map(Row.of("This is a unit test for filtering stopWords")).getField(0),
            "This unit test filtering stopWords");
        assertEquals(mapper.map(Row.of("Filter stopWords test")).getField(0),
            "Filter stopWords test");
        assertEquals(mapper.getOutputSchema(), schema);
    }
}
