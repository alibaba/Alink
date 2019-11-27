package com.alibaba.alink.operator.common.statistics.basicstatistic;

import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.model.SimpleModelDataConverter;
import com.alibaba.alink.common.utils.JsonConverter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * vector summary data converter.
 */
public class VectorSummaryDataConverter extends SimpleModelDataConverter<BaseVectorSummary, BaseVectorSummary> {

    private static final String SPARSE = "sparse";
    private static final String DENSE = "dense";

    /**
     * summary data converter.
     */
    public VectorSummaryDataConverter() {
    }


    /**
     * serialize summary.
     */
    @Override
    public Tuple2<Params, Iterable<String>> serializeModel(BaseVectorSummary summary) {
        if (summary == null) {
            return null;
        }

        List<String> data = new ArrayList<>();

        if (summary instanceof DenseVectorSummary) {
            DenseVectorSummary denseSummary = (DenseVectorSummary) summary;

            data.add(DENSE);
            data.add(VectorUtil.toString(denseSummary.sum));
            data.add(VectorUtil.toString(denseSummary.squareSum));
            data.add(VectorUtil.toString(denseSummary.min));
            data.add(VectorUtil.toString(denseSummary.max));
            data.add(VectorUtil.toString(denseSummary.normL1));
            data.add(JsonConverter.toJson(denseSummary.count));
        } else {
            SparseVectorSummary sparseSummary = (SparseVectorSummary) summary;

            data.add(SPARSE);
            data.add(JsonConverter.toJson(sparseSummary.count));
            for (Map.Entry<Integer, VectorStatCol> col : sparseSummary.cols.entrySet()) {
                data.add(JsonConverter.toJson(col.getKey()));
                data.add(JsonConverter.toJson(col.getValue()));
            }
        }

        return Tuple2.of(new Params(), data);
    }

    /**
     * Deserialize summary.
     */
    @Override
    public BaseVectorSummary deserializeModel(Params meta, Iterable<String> data) {
        if (data == null) {
            return null;
        }
        Iterator<String> dataIterator = data.iterator();
        if (!dataIterator.hasNext()) {
            return null;
        }

        String vecType = dataIterator.next();
        if (SPARSE.equals(vecType)) {
            SparseVectorSummary summary = new SparseVectorSummary();
            summary.count = JsonConverter.fromJson(dataIterator.next(), long.class);
            while (dataIterator.hasNext()) {
                int colIdx = JsonConverter.fromJson(dataIterator.next(), Integer.class);
                VectorStatCol colStat = JsonConverter.fromJson(dataIterator.next(), VectorStatCol.class);
                summary.cols.put(colIdx, colStat);
            }
            return summary;

        } else {
            DenseVectorSummary summary = new DenseVectorSummary();

            summary.sum = VectorUtil.parseDense(dataIterator.next());
            summary.squareSum = VectorUtil.parseDense(dataIterator.next());
            summary.min = VectorUtil.parseDense(dataIterator.next());
            summary.max = VectorUtil.parseDense(dataIterator.next());
            summary.normL1 = VectorUtil.parseDense(dataIterator.next());
            summary.count = JsonConverter.fromJson(dataIterator.next(), long.class);

            return summary;
        }
    }
}
