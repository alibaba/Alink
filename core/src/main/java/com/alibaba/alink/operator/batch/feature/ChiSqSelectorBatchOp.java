package com.alibaba.alink.operator.batch.feature;

import com.alibaba.alink.operator.common.feature.ChiSqSelectorModelDataConverter;
import com.alibaba.alink.operator.common.statistics.ChiSquareTestUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.feature.ChiSqSelectorParams;
import org.apache.flink.util.Preconditions;


public final class ChiSqSelectorBatchOp extends BatchOperator<ChiSqSelectorBatchOp>
    implements ChiSqSelectorParams<ChiSqSelectorBatchOp> {

    public ChiSqSelectorBatchOp() {
        super(null);
    }

    public ChiSqSelectorBatchOp(Params params) {
        super(params);
    }

    @Override
    public ChiSqSelectorBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        String[] selectedColNames = getSelectedCols();
        String labelColName = getLabelCol();

        String selectorType = getParams().get(SELECTOR_TYPE).trim().toLowerCase();
        int numTopFeatures = getParams().get(NUM_TOP_FEATURES);
        double percentile = getParams().get(PERCENTILE);
        double fpr = getParams().get(FPR);
        double fdr = getParams().get(FDR);
        double fwe = getParams().get(FWE);

        setOutputTable(ChiSquareTestUtil.selector(in, selectedColNames, labelColName,
            selectorType, numTopFeatures, percentile, fpr, fdr, fwe));

        return this;
    }


    public String[] collectResult() {
        Preconditions.checkArgument(null != this.getOutputTable(), "Please link from or link to.");

        int[] indices = new ChiSqSelectorModelDataConverter().load(this.collect());

        String[] selectedColNames = new String[indices.length];
        for (int i = 0; i < indices.length; i++) {
            selectedColNames[i] = this.getSelectedCols()[i];
        }
        return selectedColNames;
    }

}
