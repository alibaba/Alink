package com.alibaba.alink.operator.common.fm;

import java.util.List;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.batch.BatchOperator;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

/**
 * FmRegressorModelInfoBatchOp can be linked to the output of BaseFmTrainBatchOp to info the Fm model.
 */
public class FmRegressorModelInfoBatchOp
    extends ExtractModelInfoBatchOp<FmRegressorModelInfo, FmRegressorModelInfoBatchOp> {
    private TypeInformation labelType;
    public FmRegressorModelInfoBatchOp(TypeInformation labelType) {
        this(new Params());
        this.labelType = labelType;
    }

    /**
     * construct function.
     * @param params
     */
    public FmRegressorModelInfoBatchOp(Params params) {
        super(params);
    }

    @Override
    protected FmRegressorModelInfo createModelInfo(List<Row> rows) {
        return new FmRegressorModelInfo(rows, labelType);
    }

    @Override
    protected BatchOperator<?> processModel() {
        return this;
    }
}
