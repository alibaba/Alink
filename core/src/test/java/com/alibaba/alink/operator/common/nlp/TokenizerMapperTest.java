package com.alibaba.alink.operator.common.nlp;

import com.alibaba.alink.params.nlp.TokenizerParams;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for TokenizerMapper.
 */
public class TokenizerMapperTest {
    @Test
    public void testDefault() throws Exception {
        TableSchema schema = new TableSchema(new String[] {"sentence"}, new TypeInformation<?>[] {Types.STRING});

        Params params = new Params()
            .set(TokenizerParams.SELECTED_COL, "sentence");

        TokenizerMapper mapper = new TokenizerMapper(schema, params);

        assertEquals(mapper.map(Row.of("This\tis  a unit test for mapper")).getField(0),
            "this is  a unit test for mapper");
        assertEquals(mapper.getOutputSchema(), schema);
    }

}
