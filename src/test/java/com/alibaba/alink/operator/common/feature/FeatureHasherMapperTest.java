package com.alibaba.alink.operator.common.feature;

import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.params.feature.FeatureHasherParams;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for FeatureHasherMapper.
 */
public class FeatureHasherMapperTest {
    @Test
    public void test1() throws Exception {
        TableSchema schema = new TableSchema(new String[] {"double", "bool", "number", "str"},
            new TypeInformation<?>[] {Types.DOUBLE(), Types.BOOLEAN(), Types.STRING(), Types.STRING()});

        Params params = new Params()
            .set(FeatureHasherParams.SELECTED_COLS, new String[] {"double", "bool", "number", "str"})
            .set(FeatureHasherParams.OUTPUT_COL, "output")
            .set(FeatureHasherParams.RESERVED_COLS, new String[] {});

        FeatureHasherMapper mapper = new FeatureHasherMapper(schema, params);

        assertEquals(mapper.map(Row.of(1.1, true, "2", "A")).getField(0),
            new SparseVector(262144, new int[]{62393, 85133, 120275, 214318}, new double[]{1.0, 1.0, 1.0, 1.1}));
        assertEquals(mapper.map(Row.of(2.1, true, "1", "A")).getField(0),
            new SparseVector(262144, new int[]{76287, 85133, 120275, 214318}, new double[]{1.0, 1.0, 1.0, 2.1}));
        assertEquals(mapper.getOutputSchema(),
            new TableSchema(new String[] {"output"}, new TypeInformation<?>[] {VectorTypes.VECTOR})
        );
    }

    @Test
    public void test2() throws Exception {
        TableSchema schema = new TableSchema(new String[] {"double", "bool", "number", "str"},
            new TypeInformation<?>[] {Types.DOUBLE(), Types.BOOLEAN(), Types.STRING(), Types.STRING()});

        Params params = new Params()
            .set(FeatureHasherParams.SELECTED_COLS, new String[] {"double", "bool", "number", "str"})
            .set(FeatureHasherParams.OUTPUT_COL, "output")
            .set(FeatureHasherParams.NUM_FEATURES, 10);

        FeatureHasherMapper mapper = new FeatureHasherMapper(schema, params);

        assertEquals(mapper.map(Row.of(1.1, true, "2", "A")).getField(4),
            new SparseVector(10, new int[]{5, 8, 9}, new double[]{2.0, 1.1, 1.0}));
        assertEquals(mapper.map(Row.of(2.1, true, "1", "B")).getField(4),
            new SparseVector(10, new int[]{1, 5, 6, 8}, new double[]{1.0, 1.0, 1.0, 2.1}));
        assertEquals(mapper.getOutputSchema(),
            new TableSchema(new String[] {"double", "bool", "number", "str", "output"},
                new TypeInformation<?>[] {Types.DOUBLE(), Types.BOOLEAN(), Types.STRING(), Types.STRING(),
                    VectorTypes.VECTOR}));
    }

    @Test
    public void test3() throws Exception {
        TableSchema schema = new TableSchema(new String[] {"double", "bool", "number", "str"},
            new TypeInformation<?>[] {Types.DOUBLE(), Types.BOOLEAN(), Types.STRING(), Types.STRING()});

        Params params = new Params()
            .set(FeatureHasherParams.SELECTED_COLS, new String[] {"double", "bool", "number", "str"})
            .set(FeatureHasherParams.OUTPUT_COL, "output")
            .set(FeatureHasherParams.NUM_FEATURES, 10)
            .set(FeatureHasherParams.CATEGORICAL_COLS, new String[] {"double"});

        FeatureHasherMapper mapper = new FeatureHasherMapper(schema, params);

        assertEquals(mapper.map(Row.of(1.1, true, "2", "A")).getField(4),
            new SparseVector(10, new int[]{0, 5, 9}, new double[]{1.0, 2.0, 1.0}));
        assertEquals(mapper.map(Row.of(2.1, true, "1", "B")).getField(4),
            new SparseVector(10, new int[]{1, 5, 6}, new double[]{2.0, 1.0, 1.0}));
    }
}
