package com.alibaba.alink.operator.common.feature;

import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.params.feature.BinarizerParams;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for BinarizerMapper.
 */
public class BinarizerMapperTest {
    @Test
    public void test1() throws Exception {
        TableSchema schema = new TableSchema(new String[] {"feature"}, new TypeInformation<?>[] {VectorTypes.VECTOR});

        Params params = new Params()
            .set(BinarizerParams.SELECTED_COL, "feature");

        BinarizerMapper mapper = new BinarizerMapper(schema, params);

        assertEquals(mapper.map(Row.of(VectorUtil.getVector("0.1 0.6"))).getField(0), VectorUtil.getVector("1.0 1.0"));
        assertEquals(mapper.map(Row.of(VectorUtil.getVector("$20$4:0.2 6:1.0 7:0.05"))).getField(0),
            VectorUtil.getVector("$20$4:1.0 6:1.0 7:1.0"));
        assertEquals(mapper.getOutputSchema(),
            new TableSchema(new String[] {"feature"}, new TypeInformation<?>[] {VectorTypes.VECTOR}));
    }

    @Test
    public void test2() throws Exception {
        TableSchema schema = new TableSchema(new String[] {"feature"}, new TypeInformation<?>[] {VectorTypes.VECTOR});

        Params params = new Params()
            .set(BinarizerParams.SELECTED_COL, "feature")
            .set(BinarizerParams.THRESHOLD, 1.5);

        BinarizerMapper mapper = new BinarizerMapper(schema, params);

        assertEquals(mapper.map(Row.of(VectorUtil.getVector("0.1 0.6"))).getField(0), VectorUtil.getVector("$2$"));
        assertEquals(mapper.map(Row.of(VectorUtil.getVector("2.1 2.6 4.1 0.6 3.2"))).getField(0), VectorUtil.getVector("1.0 1.0 1.0 0.0 1.0"));
        assertEquals(mapper.map(Row.of(VectorUtil.getVector("$20$4:0.2 6:1.0 7:0.05"))).getField(0), VectorUtil.getVector("$20$"));

        assertEquals(mapper.getOutputSchema(),
            new TableSchema(new String[] {"feature"}, new TypeInformation<?>[] {VectorTypes.VECTOR}));
    }

    @Test
    public void test3() throws Exception {
        TableSchema schema = new TableSchema(new String[] {"feature"}, new TypeInformation<?>[] {Types.DOUBLE});

        Params params = new Params()
            .set(BinarizerParams.SELECTED_COL, "feature");

        BinarizerMapper mapper = new BinarizerMapper(schema, params);

        assertEquals(mapper.map(Row.of(0.6)).getField(0), 1.0);
        assertEquals(mapper.getOutputSchema(), schema);
    }

    @Test
    public void test4() throws Exception {
        TableSchema schema = new TableSchema(new String[] {"feature"}, new TypeInformation<?>[] {Types.DOUBLE});

        Params params = new Params()
            .set(BinarizerParams.SELECTED_COL, "feature")
            .set(BinarizerParams.OUTPUT_COL, "output")
            .set(BinarizerParams.THRESHOLD, 1.0);

        BinarizerMapper mapper = new BinarizerMapper(schema, params);

        assertEquals(mapper.map(Row.of(0.6)).getField(1), 0.0);
        assertEquals(mapper.getOutputSchema(),
            new TableSchema(new String[] {"feature", "output"},
                new TypeInformation<?>[] {Types.DOUBLE, Types.DOUBLE})
        );
    }

    @Test
    public void test5() throws Exception {
        TableSchema schema = new TableSchema(new String[] {"feature"}, new TypeInformation<?>[] {Types.LONG});

        Params params = new Params()
            .set(BinarizerParams.SELECTED_COL, "feature");

        BinarizerMapper mapper = new BinarizerMapper(schema, params);

        assertEquals(mapper.map(Row.of(4L)).getField(0), 1L);
        assertEquals(mapper.getOutputSchema(), schema);
    }
}
