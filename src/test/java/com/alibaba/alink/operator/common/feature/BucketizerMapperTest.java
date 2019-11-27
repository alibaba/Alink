package com.alibaba.alink.operator.common.feature;

import com.alibaba.alink.params.feature.BucketizerParams;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for BucketizerMapper.
 */
public class BucketizerMapperTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private String splits = "-999.9: -0.5: 0.0: 0.5: 999.9";
    private String[] splitsArray = new String[] {"-Infinity: -0.5: 0.0: 0.5: Infinity",
        "-Infinity: -0.3: 0.0: 0.3: 0.4: Infinity"};

    @Test
    public void testOneFeature() throws Exception {
        TableSchema schema = new TableSchema(new String[] {"feature"}, new TypeInformation<?>[] {Types.INT});

        Params params = new Params()
            .set(BucketizerParams.SELECTED_COLS, new String[] {"feature"})
            .set(BucketizerParams.SPLITS_ARRAY, new String[] {splits});

        BucketizerMapper mapper = new BucketizerMapper(schema, params);
        assertEquals(mapper.map(Row.of(-999.9)).getField(0), 0);
        assertEquals(mapper.map(Row.of(-0.5)).getField(0), 1);
        assertEquals(mapper.map(Row.of(-0.3)).getField(0), 1);
        assertEquals(mapper.map(Row.of(0.0)).getField(0), 2);
        assertEquals(mapper.map(Row.of(0.5)).getField(0), 3);
        assertEquals(mapper.map(Row.of(999.9)).getField(0), 3);
        assertEquals(mapper.getOutputSchema(), schema);
    }

    @Test
    public void testMultiFeatures() throws Exception {
        TableSchema schema = new TableSchema(new String[] {"featureA", "featureB"},
            new TypeInformation<?>[] {Types.INT, Types.INT});

        Params params = new Params()
            .set(BucketizerParams.SELECTED_COLS, new String[] {"featureA", "featureB"})
            .set(BucketizerParams.SPLITS_ARRAY, splitsArray);

        BucketizerMapper mapper = new BucketizerMapper(schema, params);
        assertEquals(mapper.map(Row.of(-999.9, -999.9)).getField(1), 0);
        assertEquals(mapper.map(Row.of(-0.5, -0.2)).getField(1), 1);
        assertEquals(mapper.map(Row.of(-0.3, -0.6)).getField(1), 0);
        assertEquals(mapper.map(Row.of(0.0, 0.0)).getField(1), 2);
        assertEquals(mapper.map(Row.of(0.5, 0.4)).getField(1), 4);
        assertEquals(mapper.getOutputSchema(), schema);
    }

    @Test
    public void testOutput(){
        TableSchema schema = new TableSchema(new String[] {"featureA", "featureB"},
            new TypeInformation<?>[] {Types.DOUBLE, Types.DOUBLE});

        Params params = new Params()
            .set(BucketizerParams.SELECTED_COLS, new String[] {"featureA", "featureB"})
            .set(BucketizerParams.OUTPUT_COLS, new String[] {"outputA", "outputB"})
            .set(BucketizerParams.SPLITS_ARRAY, splitsArray);

        BucketizerMapper mapper = new BucketizerMapper(schema, params);

        assertEquals(
            mapper.getOutputSchema(),
            new TableSchema(
                new String[] {"featureA", "featureB", "outputA", "outputB"},
                new TypeInformation<?>[] {Types.DOUBLE, Types.DOUBLE, Types.INT, Types.INT}
            )
        );
    }
    

    @Test
    public void testException() throws Exception {
        String exceptionSplits1 = "-999.9: -0.5";
        String exceptionSplits2 = "-0.5: -0.4: -1.0";

        TableSchema schema = new TableSchema(new String[] {"feature"}, new TypeInformation<?>[] {Types.DOUBLE});

        Params params = new Params()
            .set(BucketizerParams.SELECTED_COLS, new String[] {"feature"})
            .set(BucketizerParams.OUTPUT_COLS, new String[] {"output"})
            .set(BucketizerParams.SPLITS_ARRAY, new String[] {exceptionSplits1});

        thrown.expect(IllegalArgumentException.class);
        BucketizerMapper mapper = new BucketizerMapper(schema, params);

        params.set(BucketizerParams.SPLITS_ARRAY, new String[] {exceptionSplits2});

        thrown.expect(IllegalArgumentException.class);
        mapper = new BucketizerMapper(schema, params);

        params.set(BucketizerParams.SPLITS_ARRAY, new String[]{splits});
        thrown.expect(IllegalArgumentException.class);
        mapper.map(Row.of(Double.NaN));
    }
}
