package com.alibaba.alink.operator.common.nlp;

import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.params.nlp.DocCountVectorizerTrainParams;
import com.alibaba.alink.params.nlp.DocHashCountVectorizerPredictParams;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for DocHashIDFVectorizerModelMapper.
 */
public class DocHashCountVectorizerModelMapperTest {
    private TableSchema modelSchema = new DocHashCountVectorizerModelDataConverter().getModelSchema();
    private TableSchema dataSchema = new TableSchema(new String[] {"sentence"}, new TypeInformation<?>[] {Types.STRING});

    @Test
    public void testTFIDF() throws Exception {
        Row[] rows = new Row[] {
            Row.of(0L, "{\"numFeatures\":\"20\",\"minTF\":\"1.0\",\"featureType\":\"\\\"TF_IDF\\\"\"}"),
            Row.of(1048576L, "{\"16\":0.4054651081081644,\"7\":0.0,\"13\":0.4054651081081644,\"14\":-0.5108256237659907,"
                + "\"15\":-0.2876820724517809}")
        };
        List<Row> model = Arrays.asList(rows);

        Params params = new Params()
            .set(DocHashCountVectorizerPredictParams.SELECTED_COL, "sentence");

        DocHashCountVectorizerModelMapper mapper = new DocHashCountVectorizerModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);

        assertEquals(mapper.map(Row.of("a b c d a a ")).getField(0),
            new SparseVector(20, new int[] {7, 13, 14, 15},
                new double[] {0.0, 0.06757751801802739, -0.25541281188299536, -0.047947012075296815}));
        assertEquals(mapper.getOutputSchema(),
            new TableSchema(new String[] {"sentence"}, new TypeInformation[] {VectorTypes.SPARSE_VECTOR}));
    }

    @Test
    public void testWordCount() throws Exception {
        Row[] rows = new Row[] {
            Row.of(0L, "{\"numFeatures\":\"20\",\"minTF\":\"1.0\",\"featureType\":\"\\\"WORD_COUNT\\\"\"}"),
            Row.of(1048576L, "{\"16\":0.4054651081081644,\"7\":0.0,\"13\":0.4054651081081644,\"14\":-0.5108256237659907,"
                + "\"15\":-0.2876820724517809}")
        };
        List<Row> model = Arrays.asList(rows);

        Params params = new Params()
            .set(DocHashCountVectorizerPredictParams.SELECTED_COL, "sentence");

        DocHashCountVectorizerModelMapper mapper = new DocHashCountVectorizerModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);

        assertEquals(mapper.map(Row.of("a b c d a a ")).getField(0),
            new SparseVector(20, new int[] {7, 13, 14, 15},
                new double[] {1.0, 1.0, 3.0, 1.0}));
    }

    @Test
    public void testTF() throws Exception {
        Row[] rows = new Row[] {
            Row.of(0L, "{\"numFeatures\":\"20\",\"minTF\":\"1.0\",\"featureType\":\"\\\"TF\\\"\"}"),
            Row.of(1048576L, "{\"16\":0.4054651081081644,\"7\":0.0,\"13\":0.4054651081081644,\"14\":-0.5108256237659907,"
                + "\"15\":-0.2876820724517809}")
        };
        List<Row> model = Arrays.asList(rows);

        Params params = new Params()
            .set(DocHashCountVectorizerPredictParams.SELECTED_COL, "sentence");

        DocHashCountVectorizerModelMapper mapper = new DocHashCountVectorizerModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);

        assertEquals(mapper.map(Row.of("a b c d a a ")).getField(0),
            new SparseVector(20, new int[] {7, 13, 14, 15},
                new double[] {0.16666666666666666, 0.16666666666666666, 0.5, 0.16666666666666666}));
    }

    @Test
    public void testBinary() throws Exception {
        Row[] rows = new Row[] {
            Row.of(0L, "{\"numFeatures\":\"20\",\"minTF\":\"1.0\",\"featureType\":\"\\\"BINARY\\\"\"}"),
            Row.of(1048576L, "{\"16\":0.4054651081081644,\"7\":0.0,\"13\":0.4054651081081644,\"14\":-0.5108256237659907,"
                + "\"15\":-0.2876820724517809}")
        };
        List<Row> model = Arrays.asList(rows);

        Params params = new Params()
            .set(DocHashCountVectorizerPredictParams.SELECTED_COL, "sentence");

        DocHashCountVectorizerModelMapper mapper = new DocHashCountVectorizerModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);

        assertEquals(mapper.map(Row.of("a b c d a a ")).getField(0),
            new SparseVector(20, new int[] {7, 13, 14, 15},
                new double[] {1.0, 1.0, 1.0, 1.0}));
        }

    @Test
    public void testIDF() throws Exception {
        Row[] rows = new Row[] {
            Row.of(0L, "{\"numFeatures\":\"20\",\"minTF\":\"1.0\",\"featureType\":\"\\\"IDF\\\"\"}"),
            Row.of(1048576L, "{\"16\":0.4054651081081644,\"7\":0.0,\"13\":0.4054651081081644,\"14\":-0.5108256237659907,"
                + "\"15\":-0.2876820724517809}")
        };
        List<Row> model = Arrays.asList(rows);

        Params params = new Params()
            .set(DocHashCountVectorizerPredictParams.SELECTED_COL, "sentence")
            .set(DocCountVectorizerTrainParams.FEATURE_TYPE, "IDF");

        DocHashCountVectorizerModelMapper mapper = new DocHashCountVectorizerModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);

        assertEquals(mapper.map(Row.of("a b c d a a ")).getField(0),
            new SparseVector(20, new int[] {7, 13, 14, 15},
                new double[] {0.0, 0.4054651081081644, -0.5108256237659907, -0.2876820724517809}));
    }

}
