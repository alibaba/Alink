package com.alibaba.alink.operator.common.nlp;

import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.params.nlp.DocCountVectorizerPredictParams;
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
 * Unit test for DocCountVectorizer.
 */
public class DocCountVectorizerModelMapperTest {
    private TableSchema modelSchema = new DocCountVectorizerModelDataConverter().getModelSchema();
    private TableSchema dataSchema = new TableSchema(new String[] {"sentence"}, new TypeInformation<?>[] {Types.STRING});

    @Test
    public void testWordCountType() throws Exception {
        Row[] rows = new Row[] {
            Row.of(0L, "{\"minTF\":\"1.0\",\"featureType\":\"\\\"WORD_COUNT\\\"\"}"),
            Row.of(1048576L, "{\"f0\":\"i\",\"f1\":0.6931471805599453,\"f2\":6}"),
            Row.of(2097152L, "{\"f0\":\"e\",\"f1\":0.1823215567939546,\"f2\":2}"),
            Row.of(3145728L, "{\"f0\":\"a\",\"f1\":0.4054651081081644,\"f2\":0}"),
            Row.of(4194304L, "{\"f0\":\"b\",\"f1\":0.1823215567939546,\"f2\":1}"),
            Row.of(5242880L, "{\"f0\":\"c\",\"f1\":0.6931471805599453,\"f2\":7}"),
            Row.of(6291456L, "{\"f0\":\"h\",\"f1\":0.4054651081081644,\"f2\":3}"),
            Row.of(7340032L, "{\"f0\":\"d\",\"f1\":0.6931471805599453,\"f2\":4}"),
            Row.of(8388608L, "{\"f0\":\"j\",\"f1\":0.6931471805599453,\"f2\":5}"),
            Row.of(9437184L, "{\"f0\":\"g\",\"f1\":0.6931471805599453,\"f2\":8}"),
            Row.of(10485760L, "{\"f0\":\"n\",\"f1\":1.0986122886681098,\"f2\":9}"),
            Row.of(11534336L, "{\"f0\":\"f\",\"f1\":1.0986122886681098,\"f2\":10}")
        };
        List<Row> model = Arrays.asList(rows);

        Params params = new Params()
            .set(DocCountVectorizerPredictParams.SELECTED_COL, "sentence");

        DocCountVectorizerModelMapper mapper = new DocCountVectorizerModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);

        assertEquals(mapper.map(Row.of("a b c d e a a")).getField(0),
            new SparseVector(11, new int[] {0, 1, 2, 4, 7}, new double[] {3.0, 1.0, 1.0, 1.0, 1.0}));
        assertEquals(mapper.getOutputSchema(),
            new TableSchema(new String[] {"sentence"}, new TypeInformation[] {VectorTypes.SPARSE_VECTOR}));
    }

    @Test
    public void testTFIDFType() throws Exception {
        Row[] rows = new Row[] {
            Row.of(0L, "{\"minTF\":\"1.0\",\"featureType\":\"\\\"TF_IDF\\\"\"}"),
            Row.of(1048576L, "{\"f0\":\"i\",\"f1\":0.6931471805599453,\"f2\":6}"),
            Row.of(2097152L, "{\"f0\":\"e\",\"f1\":0.1823215567939546,\"f2\":2}"),
            Row.of(3145728L, "{\"f0\":\"a\",\"f1\":0.4054651081081644,\"f2\":0}"),
            Row.of(4194304L, "{\"f0\":\"b\",\"f1\":0.1823215567939546,\"f2\":1}"),
            Row.of(5242880L, "{\"f0\":\"c\",\"f1\":0.6931471805599453,\"f2\":7}"),
            Row.of(6291456L, "{\"f0\":\"h\",\"f1\":0.4054651081081644,\"f2\":3}"),
            Row.of(7340032L, "{\"f0\":\"d\",\"f1\":0.6931471805599453,\"f2\":4}"),
            Row.of(8388608L, "{\"f0\":\"j\",\"f1\":0.6931471805599453,\"f2\":5}"),
            Row.of(9437184L, "{\"f0\":\"g\",\"f1\":0.6931471805599453,\"f2\":8}"),
            Row.of(10485760L, "{\"f0\":\"n\",\"f1\":1.0986122886681098,\"f2\":9}"),
            Row.of(11534336L, "{\"f0\":\"f\",\"f1\":1.0986122886681098,\"f2\":10}")
        };
        List<Row> model = Arrays.asList(rows);

        Params params = new Params()
            .set(DocCountVectorizerPredictParams.SELECTED_COL, "sentence");

        DocCountVectorizerModelMapper mapper = new DocCountVectorizerModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);

        assertEquals(mapper.map(Row.of("a b c d e")).getField(0),
            new SparseVector(11, new int[] {0, 1, 2, 4, 7},
                new double[] {0.08109302162163289, 0.03646431135879092, 0.03646431135879092, 0.13862943611198905,
                    0.13862943611198905}));
    }

    @Test
    public void testOutputCol() throws Exception {
        Row[] rows = new Row[] {
            Row.of(0L, "{\"minTF\":\"1.0\",\"featureType\":\"\\\"TF\\\"\"}"),
            Row.of(1048576L, "{\"f0\":\"i\",\"f1\":0.6931471805599453,\"f2\":6}"),
            Row.of(2097152L, "{\"f0\":\"e\",\"f1\":0.1823215567939546,\"f2\":2}"),
            Row.of(3145728L, "{\"f0\":\"a\",\"f1\":0.4054651081081644,\"f2\":0}"),
            Row.of(4194304L, "{\"f0\":\"b\",\"f1\":0.1823215567939546,\"f2\":1}"),
            Row.of(5242880L, "{\"f0\":\"c\",\"f1\":0.6931471805599453,\"f2\":7}"),
            Row.of(6291456L, "{\"f0\":\"h\",\"f1\":0.4054651081081644,\"f2\":3}"),
            Row.of(7340032L, "{\"f0\":\"d\",\"f1\":0.6931471805599453,\"f2\":4}"),
            Row.of(8388608L, "{\"f0\":\"j\",\"f1\":0.6931471805599453,\"f2\":5}"),
            Row.of(9437184L, "{\"f0\":\"g\",\"f1\":0.6931471805599453,\"f2\":8}"),
            Row.of(10485760L, "{\"f0\":\"n\",\"f1\":1.0986122886681098,\"f2\":9}"),
            Row.of(11534336L, "{\"f0\":\"f\",\"f1\":1.0986122886681098,\"f2\":10}")
        };
        List<Row> model = Arrays.asList(rows);

        Params params = new Params()
            .set(DocCountVectorizerPredictParams.SELECTED_COL, "sentence")
            .set(DocCountVectorizerPredictParams.OUTPUT_COL, "output");

        DocCountVectorizerModelMapper mapper = new DocCountVectorizerModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);

        assertEquals(mapper.map(Row.of("a b c d e")).getField(1),
            new SparseVector(11, new int[] {0, 1, 2, 4, 7}, new double[] {0.2, 0.2, 0.2, 0.2, 0.2}));
    }

    @Test
    public void testMinTF() throws Exception {
        Row[] rows = new Row[] {
            Row.of(0L, "{\"minTF\":\"0.2\",\"featureType\":\"\\\"BINARY\\\"\"}"),
            Row.of(1048576L, "{\"f0\":\"i\",\"f1\":0.6931471805599453,\"f2\":6}"),
            Row.of(2097152L, "{\"f0\":\"e\",\"f1\":0.1823215567939546,\"f2\":2}"),
            Row.of(3145728L, "{\"f0\":\"a\",\"f1\":0.4054651081081644,\"f2\":0}"),
            Row.of(4194304L, "{\"f0\":\"b\",\"f1\":0.1823215567939546,\"f2\":1}"),
            Row.of(5242880L, "{\"f0\":\"c\",\"f1\":0.6931471805599453,\"f2\":7}"),
            Row.of(6291456L, "{\"f0\":\"h\",\"f1\":0.4054651081081644,\"f2\":3}"),
            Row.of(7340032L, "{\"f0\":\"d\",\"f1\":0.6931471805599453,\"f2\":4}"),
            Row.of(8388608L, "{\"f0\":\"j\",\"f1\":0.6931471805599453,\"f2\":5}"),
            Row.of(9437184L, "{\"f0\":\"g\",\"f1\":0.6931471805599453,\"f2\":8}"),
            Row.of(10485760L, "{\"f0\":\"n\",\"f1\":1.0986122886681098,\"f2\":9}"),
            Row.of(11534336L, "{\"f0\":\"f\",\"f1\":1.0986122886681098,\"f2\":10}")
        };
        List<Row> model = Arrays.asList(rows);

        Params params = new Params()
            .set(DocCountVectorizerPredictParams.SELECTED_COL, "sentence");

        DocCountVectorizerModelMapper mapper = new DocCountVectorizerModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);

        assertEquals(mapper.map(Row.of("a b c d e a a b e")).getField(0),
            new SparseVector(11, new int[] {0, 1, 2}, new double[] {1.0, 1.0, 1.0}));
        assertEquals(mapper.map(Row.of("a b c d")).getField(0),
            new SparseVector(11, new int[] {0, 1, 4, 7}, new double[] {1.0, 1.0, 1.0, 1.0}));
    }
}
