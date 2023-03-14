package com.alibaba.alink.operator.stream.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.ItemCfRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseUsersPerItemRecommParams;

/**
 * Recommend users for item with itemCF model.
 */
@NameEn("ItemCf：Users Per Item Recomm")
@NameCn("ItemCf：UsersPerItem推荐")
public class ItemCfUsersPerItemRecommStreamOp
	extends BaseRecommStreamOp <ItemCfUsersPerItemRecommStreamOp>
	implements BaseUsersPerItemRecommParams <ItemCfUsersPerItemRecommStreamOp> {

	private static final long serialVersionUID = -9021498920170224399L;

	public ItemCfUsersPerItemRecommStreamOp(BatchOperator <?> model) {
		this(model, null);
	}

	public ItemCfUsersPerItemRecommStreamOp(BatchOperator <?> model, Params params) {
		super(model, ItemCfRecommKernel::new, RecommType.USERS_PER_ITEM, params);
	}
}
