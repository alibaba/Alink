package com.alibaba.alink.operator.common.nlp;

import com.alibaba.alink.params.nlp.RegexTokenizerParams;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for RegexTokenizerMapper.
 */
public class RegexTokenizerMapperTest {
    @Test
    public void testDefault() throws Exception {
        TableSchema schema = new TableSchema(new String[] {"sentence"}, new TypeInformation<?>[] {Types.STRING});

        Params params = new Params()
            .set(RegexTokenizerParams.SELECTED_COL, "sentence");

        RegexTokenizerMapper mapper = new RegexTokenizerMapper(schema, params);

        assertEquals(mapper.map(Row.of("This is a unit test for mapper")).getField(0),
            "this is a unit test for mapper");
        assertEquals(mapper.getOutputSchema(), schema);
    }

    @Test
    public void testMinTokenLength() throws Exception {
        TableSchema schema = new TableSchema(new String[] {"sentence"}, new TypeInformation<?>[] {Types.STRING});

        Params params = new Params()
            .set(RegexTokenizerParams.SELECTED_COL, "sentence")
            .set(RegexTokenizerParams.MIN_TOKEN_LENGTH, 3);

        RegexTokenizerMapper mapper = new RegexTokenizerMapper(schema, params);

        assertEquals(mapper.map(Row.of("This is a unit test for mapper")).getField(0), "this unit test for mapper");
        assertEquals(mapper.getOutputSchema(), schema);
    }

    @Test
    public void testPattern() throws Exception {
        TableSchema schema = new TableSchema(new String[] {"sentence"}, new TypeInformation<?>[] {Types.STRING});

        Params params = new Params()
            .set(RegexTokenizerParams.SELECTED_COL, "sentence")
            .set(RegexTokenizerParams.GAPS, false)
            .set(RegexTokenizerParams.PATTERN, "\\W");

        RegexTokenizerMapper mapper = new RegexTokenizerMapper(schema, params);

        assertEquals(mapper.map(Row.of("This,is,a,unit,test,for,mapper!")).getField(0), ", , , , , , !");
        assertEquals(mapper.getOutputSchema(), schema);
    }

    @Test
    public void testToLowerCase() throws Exception {
        TableSchema schema = new TableSchema(new String[] {"sentence"}, new TypeInformation<?>[] {Types.STRING});

        Params params = new Params()
            .set(RegexTokenizerParams.SELECTED_COL, "sentence")
            .set(RegexTokenizerParams.TO_LOWER_CASE, true);

        RegexTokenizerMapper mapper = new RegexTokenizerMapper(schema, params);

        assertEquals(mapper.map(Row.of("This is a unit test for mapper")).getField(0),
            "this is a unit test for mapper");
        assertEquals(mapper.getOutputSchema(), schema);
    }

}
