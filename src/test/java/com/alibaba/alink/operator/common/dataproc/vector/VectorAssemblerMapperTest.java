package com.alibaba.alink.operator.common.dataproc.vector;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.params.dataproc.vector.VectorAssemblerParams;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for VectorAssemblerMapper.
 */

public class VectorAssemblerMapperTest {
    @Test
    public void testToDense() throws Exception {
        TableSchema schema = new TableSchema(new String[]{"c0", "c1", "c2"},
            new TypeInformation<?>[]{Types.STRING, Types.DOUBLE, Types.STRING});

        TableSchema outSchema = new TableSchema(new String[]{"c0", "c1", "c2", "out"},
            new TypeInformation<?>[]{Types.STRING, Types.DOUBLE, Types.STRING, VectorTypes.VECTOR});

        Params params = new Params()
            .set(VectorAssemblerParams.SELECTED_COLS, new String[]{"c0", "c1", "c2"})
            .set(VectorAssemblerParams.OUTPUT_COL, "out");

        VectorAssemblerMapper mapper = new VectorAssemblerMapper(schema, params);
        /* join the DenseVector, the number and the SparseVector together. the forth field shows the result */
        assertEquals(mapper.map(Row.of(new DenseVector(new double[]{3.0, 4.0}), 3.0, new SparseVector(3, new int[]{0, 2}, new double[]{1.0, 4.0}))).getField(3),
            new DenseVector(new double[]{3.0, 4.0, 3.0, 1.0, 0.0, 4.0}));
        assertEquals(mapper.getOutputSchema(), outSchema);
    }

    @Test
    public void testToSparse() throws Exception {
        TableSchema schema = new TableSchema(new String[]{"c0", "c1", "c2"},
            new TypeInformation<?>[]{Types.STRING, Types.DOUBLE, Types.STRING});

        TableSchema outSchema = new TableSchema(new String[]{"c0", "out"},
            new TypeInformation<?>[]{Types.STRING, VectorTypes.VECTOR});

        Params params = new Params()
            .set(VectorAssemblerParams.SELECTED_COLS, new String[]{"c0", "c1", "c2"})
            .set(VectorAssemblerParams.OUTPUT_COL, "out")
            .set(VectorAssemblerParams.RESERVED_COLS, new String[]{"c0"});

        VectorAssemblerMapper mapper = new VectorAssemblerMapper(schema, params);
        /* only reverse one column. */
        assertEquals(mapper.map(Row.of(new DenseVector(new double[]{3.0, 4.0}), 3.0, new SparseVector(11, new int[]{0, 10}, new double[]{1.0, 4.0}))).getField(1),
            new SparseVector(14, new int[]{0, 1, 2, 3, 13}, new double[]{3.0, 4.0, 3.0, 1.0, 4.0}));
        assertEquals(mapper.getOutputSchema(), outSchema);
    }

    @Test
    public void testSkip() throws Exception {
        TableSchema schema = new TableSchema(new String[]{"c0", "c1", "c2"},
            new TypeInformation<?>[]{Types.STRING, Types.DOUBLE, Types.STRING});

        TableSchema outSchema = new TableSchema(new String[]{"c0", "out"},
            new TypeInformation<?>[]{Types.STRING, VectorTypes.VECTOR});

        Params params = new Params()
            .set(VectorAssemblerParams.SELECTED_COLS, new String[]{"c0", "c1", "c2"})
            .set(VectorAssemblerParams.OUTPUT_COL, "out")
            .set(VectorAssemblerParams.HANDLE_INVALID, "skip")
            .set(VectorAssemblerParams.RESERVED_COLS, new String[]{"c0"});

        VectorAssemblerMapper mapper = new VectorAssemblerMapper(schema, params);
        /* skip the invalid data. */
        assertEquals(mapper.map(Row.of(new DenseVector(new double[]{3.0, 4.0}), null, new SparseVector(11, new int[]{0, 10}, new double[]{1.0, 4.0}))).getField(1),
            new SparseVector(13, new int[]{0, 1, 2, 12}, new double[]{3.0, 4.0, 1.0, 4.0}));
        assertEquals(mapper.getOutputSchema(), outSchema);
    }

    @Test
    public void testKeep() throws Exception {
        TableSchema schema = new TableSchema(new String[]{"c0", "c1", "c2"},
            new TypeInformation<?>[]{Types.STRING, Types.DOUBLE, Types.STRING});

        TableSchema outSchema = new TableSchema(new String[]{"c0", "out"},
            new TypeInformation<?>[]{Types.STRING, VectorTypes.VECTOR});

        Params params = new Params()
            .set(VectorAssemblerParams.SELECTED_COLS, new String[]{"c0", "c1", "c2"})
            .set(VectorAssemblerParams.OUTPUT_COL, "out")
            .set(VectorAssemblerParams.HANDLE_INVALID, "keep")
            .set(VectorAssemblerParams.RESERVED_COLS, new String[]{"c0"});

        VectorAssemblerMapper mapper = new VectorAssemblerMapper(schema, params);

        assertEquals(mapper.map(Row.of(new DenseVector(new double[]{3.0, 4.0}), null, new SparseVector(11, new int[]{0, 10}, new double[]{1.0, 4.0}))).getField(1),
            new SparseVector(14, new int[]{0, 1, 2, 3, 13}, new double[]{3.0, 4.0, Double.NaN, 1.0, 4.0}));
        assertEquals(mapper.getOutputSchema(), outSchema);
    }

    @Test
    public void testStringValue() throws Exception {
        TableSchema schema = new TableSchema(new String[]{"c0", "c1", "c2"},
            new TypeInformation<?>[]{Types.STRING, Types.DOUBLE, Types.STRING});

        TableSchema outSchema = new TableSchema(new String[]{"c0", "out"},
            new TypeInformation<?>[]{Types.STRING, VectorTypes.VECTOR});

        Params params = new Params()
            .set(VectorAssemblerParams.SELECTED_COLS, new String[]{"c0", "c1", "c2"})
            .set(VectorAssemblerParams.OUTPUT_COL, "out")
            .set(VectorAssemblerParams.HANDLE_INVALID, "keep")
            .set(VectorAssemblerParams.RESERVED_COLS, new String[]{"c0"});

        VectorAssemblerMapper mapper = new VectorAssemblerMapper(schema, params);

        assertEquals(mapper.map(Row.of(new DenseVector(new double[]{3.0, 4.0}), "1", new SparseVector(11, new int[]{0, 10}, new double[]{1.0, 4.0}))).getField(1),
            new SparseVector(14, new int[]{0, 1, 2, 3, 13}, new double[]{3.0, 4.0, 1.0, 1.0, 4.0}));
        assertEquals(mapper.getOutputSchema(), outSchema);
    }
}
