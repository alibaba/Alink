package com.alibaba.alink.operator.batch.associationrule;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.associationrule.ApplySequenceRuleModelMapper;
import com.alibaba.alink.params.mapper.SISOMapperParams;

/**
 * The batch op of applying Sequence Rules.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("序列规则预测")
@NameEn("Sequence Rule Prediction")
public class ApplySequenceRuleBatchOp extends ModelMapBatchOp <ApplySequenceRuleBatchOp>
	implements SISOMapperParams <ApplySequenceRuleBatchOp> {

	private static final long serialVersionUID = -26090263164779243L;

	public ApplySequenceRuleBatchOp() {
		this(null);
	}

	/**
	 * constructor.
	 *
	 * @param params the parameters set.
	 */
	public ApplySequenceRuleBatchOp(Params params) {
		super(ApplySequenceRuleModelMapper::new, params);
	}
}
