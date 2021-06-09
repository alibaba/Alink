package com.alibaba.alink.operator.common.aps;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;

public abstract class ApsIterator<DT, MT> {

	public abstract Tuple2 <DataSet <Tuple2 <Long, MT>>, ApsContext> train(
		IterativeDataSet <Tuple2 <Long, MT>> loop,
		DataSet <DT> miniBatch,
		ApsContext curContext,
		BatchOperator[] others,
		Params params);
}
