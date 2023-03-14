package com.alibaba.alink.operator.batch.associationrule;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.associationrule.ApplyAssociationRuleModelMapper;
import com.alibaba.alink.params.mapper.SISOMapperParams;

/**
 * The batch op of applying the Association Rules.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("关联规则预测")
@NameEn("Association Rule Prediction")
public class ApplyAssociationRuleBatchOp extends ModelMapBatchOp <ApplyAssociationRuleBatchOp>
	implements SISOMapperParams <ApplyAssociationRuleBatchOp> {

	private static final long serialVersionUID = 674848671578909834L;

	public ApplyAssociationRuleBatchOp() {
		this(null);
	}

	/**
	 * constructor.
	 *
	 * @param params the parameters set.
	 */
	public ApplyAssociationRuleBatchOp(Params params) {
		super(ApplyAssociationRuleModelMapper::new, params);
	}
}
