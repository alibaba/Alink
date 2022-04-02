package com.alibaba.alink.operator.stream.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.recommendation.FlattenKObjectMapper;
import com.alibaba.alink.operator.stream.utils.FlatMapStreamOp;
import com.alibaba.alink.params.recommendation.FlattenKObjectParams;

/**
 * Transform json format recommendation to table format.
 */
@ParamSelectColumnSpec(name = "selectedCol",
    allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("FlattenKObject")
public class FlattenKObjectStreamOp
        extends FlatMapStreamOp<FlattenKObjectStreamOp>
        implements FlattenKObjectParams<FlattenKObjectStreamOp> {

    private static final long serialVersionUID = 362590454000656600L;

    public FlattenKObjectStreamOp() {
        this(null);
    }

    public FlattenKObjectStreamOp(Params params) {
        super(FlattenKObjectMapper::new, params);
    }
}
