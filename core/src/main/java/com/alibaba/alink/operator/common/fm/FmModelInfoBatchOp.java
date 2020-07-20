package com.alibaba.alink.operator.common.fm;

import java.util.List;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.batch.BatchOperator;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

/**
 * FmModelInfoBatchOp can be linked to the output of BaseFmTrainBatchOp to summary the Fm model.
 */
public class FmModelInfoBatchOp
    extends ExtractModelInfoBatchOp<FmModelInfo, FmModelInfoBatchOp> {
    private TypeInformation labelType;
    public FmModelInfoBatchOp(TypeInformation labelType) {
        this(new Params());
        this.labelType = labelType;
    }

    /**
     * construct function.
     * @param params
     */
    public FmModelInfoBatchOp(Params params) {
        super(params);
    }

    @Override
    protected FmModelInfo createModelInfo(List<Row> rows) {
        return new FmModelInfo(rows, labelType);
    }

    @Override
    protected BatchOperator<?> processModel() {
        return this;
    }
}
