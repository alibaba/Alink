package com.alibaba.alink.operator.batch.feature;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.feature.ChiSqSelectorModelDataConverter;
import com.alibaba.alink.operator.common.statistics.ChiSquareTestUtil;
import com.alibaba.alink.params.feature.VectorChiSqSelectorParams;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.Preconditions;

public final class VectorChiSqSelectorBatchOp extends BatchOperator<VectorChiSqSelectorBatchOp>
    implements VectorChiSqSelectorParams<VectorChiSqSelectorBatchOp> {

    public VectorChiSqSelectorBatchOp() {
        super(null);
    }

    public VectorChiSqSelectorBatchOp(Params params) {
        super(params);
    }

    @Override
    public VectorChiSqSelectorBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        String vectorColName = getSelectedCol();
        String labelColName = getLabelCol();

        String selectorType = getParams().get(SELECTOR_TYPE).trim().toLowerCase();
        int numTopFeatures = getParams().get(NUM_TOP_FEATURES);
        double percentile = getParams().get(PERCENTILE);
        double fpr = getParams().get(FPR);
        double fdr = getParams().get(FDR);
        double fwe = getParams().get(FWE);

        setOutputTable(ChiSquareTestUtil.vectorSelector(in, vectorColName, labelColName,
            selectorType, numTopFeatures, percentile, fpr, fdr, fwe));

        return this;
    }

    public int[] collectResult() {
        Preconditions.checkArgument(null != this.getOutputTable(), "Please link from or link to.");
        return  new ChiSqSelectorModelDataConverter().load(this.collect());
    }

}
