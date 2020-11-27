package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.common.utils.DataSetUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.AlsModelInfo;
import com.alibaba.alink.operator.common.recommendation.HugeMfAlsImpl;
import com.alibaba.alink.operator.common.utils.PackBatchOperatorUtil;
import com.alibaba.alink.params.recommendation.AlsTrainParams;

import java.util.List;

/**
 * Matrix factorization using Alternating Least Square method.
 * <p>
 * ALS tries to decompose a matrix R as R = X * Yt. Here X and Y are called factor matrices.
 * Matrix R is usually a sparse matrix representing ratings given from users to items.
 * ALS tries to find X and Y that minimize || R - X * Yt ||^2. This is done by iterations.
 * At each step, X is fixed and Y is solved, then Y is fixed and X is solved.
 * <p>
 * The algorithm is described in "Large-scale Parallel Collaborative Filtering for the Netflix Prize, 2007"
 * <p>
 * We also support implicit preference model described in
 * "Collaborative Filtering for Implicit Feedback Datasets, 2008"
 */
public final class AlsTrainBatchOp
	extends BatchOperator <AlsTrainBatchOp>
	implements AlsTrainParams <AlsTrainBatchOp>,
	WithModelInfoBatchOp <AlsModelInfo, AlsTrainBatchOp, AlsModelInfoBatchOp> {

	private static final long serialVersionUID = 135071766504939341L;
	private BatchOperator userFactors;
	private BatchOperator itemFactors;
	private DataSet <Long> sampleCount;

	public AlsTrainBatchOp() {
		this(new Params());
	}

	public AlsTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public AlsTrainBatchOp linkFrom(List <BatchOperator <?>> ins) {
		return linkFrom(ins.get(0));
	}

	@Override
	public AlsModelInfoBatchOp getModelInfoBatchOp() {
		return new AlsModelInfoBatchOp(new Params()).linkFrom(this);
	}

	/**
	 * Matrix decomposition using ALS algorithm.
	 *
	 * @param inputs a dataset of user-item-rating tuples
	 * @return user factors and item factors.
	 */
	@Override
	public AlsTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		final String userColName = getUserCol();
		final String itemColName = getItemCol();

		Tuple2 <BatchOperator, BatchOperator> factors = HugeMfAlsImpl.factorize(in, getParams(), false);
		userFactors = factors.f0;
		itemFactors = factors.f1;

		BatchOperator[] outputs = new BatchOperator[] {factors.f0, factors.f1,
			in.select(new String[] {userColName, itemColName})};
		BatchOperator model = PackBatchOperatorUtil.packBatchOps(outputs);
		this.setOutputTable(model.getOutputTable());
		this.setSideOutputTables(new Table[] {userFactors.getOutputTable(), itemFactors.getOutputTable()});
		this.sampleCount = DataSetUtil.count(in.getDataSet());
		return this;
	}
}
