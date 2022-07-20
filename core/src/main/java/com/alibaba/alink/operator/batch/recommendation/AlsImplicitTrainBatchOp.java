package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.HugeMfAlsImpl;
import com.alibaba.alink.operator.common.utils.PackBatchOperatorUtil;
import com.alibaba.alink.params.recommendation.AlsImplicitTrainParams;

import java.util.List;

import static com.alibaba.alink.operator.batch.recommendation.AlsModelInfoBatchOp.ITEM_NAME;
import static com.alibaba.alink.operator.batch.recommendation.AlsModelInfoBatchOp.USER_NAME;

/**
 * Matrix factorization using Alternating Least Square method.
 * <p>
 * ALS tries to decompose a matrix R as R = X * Yt. Here X and Y are called factor matrices. Matrix R is usually a
 * sparse matrix representing ratings given from users to items. ALS tries to find X and Y that minimize || R - X * Yt
 * ||^2. This is done by iterations. At each step, X is fixed and Y is solved, then Y is fixed and X is solved.
 * <p>
 * The algorithm is described in "Large-scale Parallel Collaborative Filtering for the Netflix Prize, 2007"
 * <p>
 * We also support implicit preference model described in "Collaborative Filtering for Implicit Feedback Datasets,
 * 2008"
 */
@InputPorts(values = {
	@PortSpec(PortType.DATA),
	@PortSpec(value = PortType.MODEL, isOptional = true),
})
@OutputPorts(values = {
	@PortSpec(PortType.MODEL),
	@PortSpec(value = PortType.DATA, desc = PortDesc.USER_FACTOR),
	@PortSpec(value = PortType.DATA, desc = PortDesc.ITEM_FACTOR),
	@PortSpec(value = PortType.DATA, desc = PortDesc.APPEND_USER_FACTOR, isOptional = true),
	@PortSpec(value = PortType.DATA, desc = PortDesc.APPEND_ITEM_FACTOR, isOptional = true)
})
@ParamSelectColumnSpec(name = "userCol")
@ParamSelectColumnSpec(name = "itemCol")
@ParamSelectColumnSpec(name = "rateCol",
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("ALS隐式训练")
public final class AlsImplicitTrainBatchOp
	extends BatchOperator <AlsImplicitTrainBatchOp>
	implements AlsImplicitTrainParams <AlsImplicitTrainBatchOp> {

	private static final long serialVersionUID = 5432932329983325493L;

	public AlsImplicitTrainBatchOp() {
		this(new Params());
	}

	public AlsImplicitTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public AlsImplicitTrainBatchOp linkFrom(List <BatchOperator <?>> ins) {
		return linkFrom(ins.get(0));
	}

	/**
	 * Matrix decomposition using ALS algorithm.
	 *
	 * @param inputs a dataset of user-item-rating tuples
	 * @return user factors and item factors.
	 */
	@Override
	public AlsImplicitTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		final String userColName = getUserCol();
		final String itemColName = getItemCol();

		BatchOperator <?> in;
		if (inputs.length == 1) {
			in = inputs[0];
			Tuple2 <BatchOperator <?>, BatchOperator <?>> factors = HugeMfAlsImpl.factorize(in, getParams(), true);
			BatchOperator <?>[] outputs = new BatchOperator <?>[] {factors.f0, factors.f1,
				in.select(new String[] {userColName, itemColName})};
			BatchOperator <?> model = PackBatchOperatorUtil.packBatchOps(outputs);
			this.setOutputTable(model.getOutputTable());
			this.setSideOutputTables(new Table[] {factors.f0.getOutputTable(), factors.f1.getOutputTable()});
		} else if (inputs.length == 2) {
			in = inputs[1];
			AlsModelInfoBatchOp modelInfo = new AlsModelInfoBatchOp(getParams()).linkFrom(inputs[0]);
			BatchOperator <?> initUserEmbedding
				= modelInfo.getUserEmbedding().select(USER_NAME + " as " + userColName + ", factors");
			BatchOperator <?> initItemEmbedding
				= modelInfo.getItemEmbedding().select(ITEM_NAME + " as " + itemColName + ", factors");
			Tuple4 <BatchOperator <?>, BatchOperator <?>, BatchOperator <?>, BatchOperator <?>>
				factors = HugeMfAlsImpl.factorize(initUserEmbedding, initItemEmbedding, in, getParams(), true);
			BatchOperator <?>[] outputs = new BatchOperator <?>[] {factors.f0, factors.f1,
				in.select(new String[] {userColName, itemColName})};

			BatchOperator <?> model = PackBatchOperatorUtil.packBatchOps(outputs);
			this.setOutputTable(model.getOutputTable());
			this.setSideOutputTables(new Table[] {factors.f0.getOutputTable(), factors.f1.getOutputTable(),
				factors.f2.getOutputTable(), factors.f3.getOutputTable()});
		} else {
			throw new AkIllegalArgumentException("als input op count err, need 1 or 2 input op.");
		}
		return this;
	}
}
