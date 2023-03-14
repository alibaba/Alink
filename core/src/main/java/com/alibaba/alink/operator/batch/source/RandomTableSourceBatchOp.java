package com.alibaba.alink.operator.batch.source;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.common.utils.RandomTableSourceUtils;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.RandomTable;
import com.alibaba.alink.params.io.RandomTableSourceBatchParams;

import java.util.Map;

/**
 * Generate table with random data.
 * We support five random type of each column: uniform, uniform_open, gauss, weight_set and poisson.
 * uniform(1,2,nullper=0.1): uniform from 1 to 2 with 0.1 of the data is null;
 * uniform_open(1,2): uniform from 1 to 2 in the open space;
 * weight_set(1.0,3.0,2.0,5.0): random generate data of 1.0 and 2.0 while the ratio of the two is 1:2;
 * gauss(0,1): generate data from gauss(0, 1);
 * poisson(0.5): generate data from poisson distribution with lambda = 0.5 .
 */
@IoOpAnnotation(name = "random_table", ioType = IOType.SourceBatch)
@NameCn("随机生成结构数据源")
@NameEn("Random Table Source")
public final class RandomTableSourceBatchOp extends BaseSourceBatchOp <RandomTableSourceBatchOp>
	implements RandomTableSourceBatchParams <RandomTableSourceBatchOp> {

	private static final long serialVersionUID = -6171191677350106691L;

	public RandomTableSourceBatchOp() {
		this(new Params());
	}

	public RandomTableSourceBatchOp(long numRows, int numCols) {
		this(new Params().set(NUM_ROWS, numRows)
			.set(NUM_COLS, numCols));
	}

	// params : colNamesString example "f0,f1,f2,f3,f4,f5" //todo : keep array
	// params : colConfsString example "f0:uniform(0,1,nullper=0.1);f1:uniform_open(0,1)"
	public RandomTableSourceBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(RandomTableSourceBatchOp.class), params);
	}

	@Override
	public Table initializeDataSource() {
		long numRows = getNumRows();
		int numCols = getNumCols();
		String idColName = getIdCol();
		String[] colNames = getOutputCols();
		String colConfsString = getOutputColConfs();

		String[] keepColNames = idColName != null ? new String[] {idColName} : new String[] {};

		if (colNames == null) {
			colNames = new String[numCols];
			for (int i = 0; i < numCols; ++i) {
				colNames[i] = "col" + i;
			}
		}

		for (int i = 0; i < colNames.length; i++) {
			colNames[i] = colNames[i].trim();
		}

		Map <String, Tuple3 <String, Double[], Double>> colConfs = RandomTableSourceUtils.parseColConfs(colConfsString,
			colNames);

		BatchOperator<?> initData = idColName != null ? new NumSeqSourceBatchOp(0, numRows - 1, idColName, getParams())
			: new NumSeqSourceBatchOp(0L, numRows - 1, getParams());
		if (idColName == null) { idColName = "num"; }
		return initData
			.udtf(idColName, colNames,
				new RandomTable(colConfs, colNames), keepColNames)
			.getOutputTable();
	}

}
