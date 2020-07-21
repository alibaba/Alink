package com.alibaba.alink.operator.common.fm;

import java.util.List;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.batch.BatchOperator;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

/**
 * FmClassifierModelInfoBatchOp can be linked to the output of BaseFmTrainBatchOp to info the Fm model.
 */
public class FmClassifierModelInfoBatchOp
    extends ExtractModelInfoBatchOp<FmClassifierModelInfo, FmClassifierModelInfoBatchOp> {
    private TypeInformation labelType;

    public FmClassifierModelInfoBatchOp(TypeInformation labelType) {
        this(new Params());
        this.labelType = labelType;
    }

    /**
     * construct function.
     *
     * @param params
     */
    public FmClassifierModelInfoBatchOp(Params params) {
        super(params);
    }

    @Override
    protected FmClassifierModelInfo createModelInfo(List<Row> rows) {
        return new FmClassifierModelInfo(rows, labelType);
    }

    @Override
    protected BatchOperator<?> processModel() {
        return this;
    }
}
