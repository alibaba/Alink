package com.alibaba.alink.operator.batch.nlp;

import com.alibaba.alink.operator.common.nlp.DocCountVectorizerModelData;
import com.alibaba.alink.operator.common.nlp.DocCountVectorizerModelDataConverter;
import com.alibaba.alink.operator.common.nlp.DocWordSplitCount;
import com.alibaba.alink.operator.common.nlp.NLPConstant;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.SortUtils;
import com.alibaba.alink.params.nlp.DocCountVectorizerTrainParams;
import com.alibaba.alink.params.nlp.DocHashCountVectorizerTrainParams;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.alink.operator.common.nlp.WordCountUtil.localSort;

/**
 * Extract all words from the dataset.Record the document frequency(DF), word count(WC) and inverse document
 * frequency(IDF) of every word as a model.
 */
public final class DocCountVectorizerTrainBatchOp extends BatchOperator<DocCountVectorizerTrainBatchOp>
    implements DocCountVectorizerTrainParams<DocCountVectorizerTrainBatchOp> {
    private static final String WORD_COL_NAME = "word";
    private static final String DOC_WORD_COUNT_COL_NAME = "doc_word_cnt";
    private static final String DOC_COUNT_COL_NAME = "doc_cnt";

    public DocCountVectorizerTrainBatchOp() {
        this(null);
    }

    public DocCountVectorizerTrainBatchOp(Params params) {
        super(params);
    }

    @Override
    public DocCountVectorizerTrainBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        DataSet<DocCountVectorizerModelData> resDocCountModel = generateDocCountModel(getParams(), in);

        DataSet<Row> res = resDocCountModel.mapPartition(new MapPartitionFunction<DocCountVectorizerModelData, Row>() {
            @Override
            public void mapPartition(Iterable<DocCountVectorizerModelData> modelDataList, Collector<Row> collector) {
                new DocCountVectorizerModelDataConverter().save(modelDataList.iterator().next(), collector);
            }
        });
        this.setOutput(res, new DocCountVectorizerModelDataConverter().getModelSchema());

        return this;
    }

    public static DataSet<DocCountVectorizerModelData> generateDocCountModel(Params params, BatchOperator in) {
        BatchOperator<?> docWordCnt = in.udtf(
                params.get(SELECTED_COL),
                new String[] {WORD_COL_NAME, DOC_WORD_COUNT_COL_NAME},
                new DocWordSplitCount(NLPConstant.WORD_DELIMITER),
                new String[] {});
        BatchOperator docCnt = in.select("COUNT(1) AS " + DOC_COUNT_COL_NAME);

        DataSet<Row> sortInput = docWordCnt
                .select(new String[] {WORD_COL_NAME, DOC_WORD_COUNT_COL_NAME})
                .getDataSet()
                .groupBy(0)
                .reduceGroup(new CalcIdf(params.get(MAX_DF), params.get(MIN_DF)))
                .withBroadcastSet(docCnt.getDataSet(), "docCnt");

        Tuple2<DataSet<Tuple2<Integer, Row>>, DataSet<Tuple2<Integer, Long>>> partitioned = SortUtils.pSort(
                sortInput, 1);
        DataSet<Tuple2<Long, Row>> ordered = localSort(partitioned.f0, partitioned.f1, 1);

        int vocabSize = params.get(VOCAB_SIZE);
        DataSet<DocCountVectorizerModelData> resDocCountModel = ordered.filter(new FilterFunction<Tuple2<Long, Row>>() {
            @Override
            public boolean filter(Tuple2<Long, Row> value) {
                return value.f0 < vocabSize;
            }
        }).mapPartition(new BuildDocCountModel(params)).setParallelism(1);
        return resDocCountModel;
    }

    /**
     * Count the DF of every word and calculate the IDF.
     */
    private static class CalcIdf extends RichGroupReduceFunction<Row, Row> {
        private long docCnt;
        private double maxDF, minDF;

        public CalcIdf(double maxDF, double minDF) {
            this.maxDF = maxDF;
            this.minDF = minDF;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            List<Row> doc = this.getRuntimeContext().getBroadcastVariable("docCnt");
            this.docCnt = ((Number)doc.get(0).getField(0)).longValue();
            this.maxDF = this.maxDF >= 1.0 ? this.maxDF : this.maxDF * this.docCnt;
            this.minDF = this.minDF >= 1.0 ? this.minDF : this.minDF * this.docCnt;
            if (this.maxDF < this.minDF) {
                throw new IllegalArgumentException("MaxDF must be greater than MinDF!");
            }
        }

        @Override
        public void reduce(Iterable<Row> rows, Collector<Row> collector) {
            double df = 0.0;
            double wordCount = 0.0;
            Object featureName = null;
            for (Row row : rows) {
                if (null == featureName) {
                    featureName = row.getField(0);
                }
                df += 1.0;
                wordCount += ((Number)row.getField(1)).doubleValue();
            }
            if (df >= this.minDF && df <= this.maxDF) {
                double idf = Math.log((1.0 + docCnt) / (1.0 + df));
                collector.collect(Row.of(featureName, -wordCount, idf));
            }
        }
    }

    /**
     * Save the id, inverse document frequency, document frequency into DocCountVectorizerModel.
     */
    private static class BuildDocCountModel implements MapPartitionFunction<Tuple2<Long, Row>, DocCountVectorizerModelData> {
        private String featureType;
        private double minTF;

        public BuildDocCountModel(Params params) {
            this.featureType = params.get(DocHashCountVectorizerTrainParams.FEATURE_TYPE);
            this.minTF = params.get(DocHashCountVectorizerTrainParams.MIN_TF);
        }

        @Override
        public void mapPartition(Iterable<Tuple2<Long, Row>> iterable, Collector<DocCountVectorizerModelData> collector) throws Exception {
            List<String> data = new ArrayList<>();
            Tuple3<String, Double, Integer> feature = Tuple3.of(null, null, null);
            for (Tuple2<Long, Row> tuple : iterable) {
                Row row = tuple.f1;

                feature.f0 = row.getField(0).toString();
                feature.f1 = ((Number)row.getField(2)).doubleValue();
                feature.f2 = tuple.f0.intValue();

                data.add(JsonConverter.toJson(feature));
            }

            DocCountVectorizerModelData modelData = new DocCountVectorizerModelData();
            modelData.featureType = featureType;
            modelData.minTF = minTF;
            modelData.list = data;
            collector.collect(modelData);

//            new DocCountVectorizerModelDataConverter().save(modelData, collector);
        }
    }
}