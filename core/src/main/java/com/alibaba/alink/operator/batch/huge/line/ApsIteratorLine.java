package com.alibaba.alink.operator.batch.huge.line;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.aps.ApsContext;
import com.alibaba.alink.operator.common.aps.ApsIterator;
import com.alibaba.alink.operator.common.aps.ApsOp;

import java.util.Random;

public class ApsIteratorLine extends ApsIterator <Number[], float[][]> {
	@Override
	public Tuple2 <DataSet <Tuple2 <Long, float[][]>>, ApsContext> train(
		IterativeDataSet <Tuple2 <Long, float[][]>> loop,
		DataSet <Number[]> miniBatch,
		ApsContext curContext, BatchOperator[] others, Params params) {

		curContext = curContext.map(new RichMapFunction <Params, Params>() {
			private static final long serialVersionUID = -5072762110067378368L;

			@Override
			public Params map(Params context) throws Exception {
				int n = getRuntimeContext().getNumberOfParallelSubtasks();
				Long[] seeds = new Long[n];
				Random rand = new Random();
				for (int i = 0; i < n; i++) {
					seeds[i] = rand.nextLong();
				}
				return context
					.set(ApsContext.SEEDS, seeds);
			}
		});

		DataSet <Tuple2 <Long, float[][]>> model = ApsOp.pullTrainPush(miniBatch, loop,
			new ApsIndexFunc4PullLine(params), curContext,
			new ApsFuncTrainLine(params), curContext,
			new ApsFuncUpdateModelLine());
		return Tuple2.of(model, curContext);
	}
}
