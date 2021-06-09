package com.alibaba.alink.operator.batch.huge.line;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.io.directreader.DefaultDistributedInfo;
import com.alibaba.alink.common.io.directreader.DistributedInfo;
import com.alibaba.alink.operator.common.aps.ApsContext;
import com.alibaba.alink.operator.common.aps.ApsFuncIndex4Pull;
import com.alibaba.alink.params.graph.LineParams;
import com.alibaba.alink.params.nlp.HasNegative;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ApsIndexFunc4PullLine extends ApsFuncIndex4Pull <Number[]> {

	private final static Logger LOG = LoggerFactory.getLogger(ApsIndexFunc4PullLine.class);
	private static final long serialVersionUID = -3540228966146033262L;

	private final Params params;

	ApsIndexFunc4PullLine(Params params) {
		this.params = params;
	}

	@Override
	protected Set <Long> requestIndex(List <Number[]> data) throws Exception {
		long startTimeConsumption = System.nanoTime();
		LOG.info("taskId: {}, localId: {}", getPatitionId(), getRuntimeContext().getIndexOfThisSubtask());
		LOG.info("taskId: {}, negInputSize: {}", getPatitionId(), data.size());

		if (null != this.contextParams) {
			Long[] nsPool = this.contextParams.getLongArray("negBound");
			int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
			Long[] seeds = this.contextParams.get(ApsContext.SEEDS);
			int seed = seeds[subTaskId].intValue();
			int negaTime = params.get(HasNegative.NEGATIVE);
			int threadNum = params.getIntegerOrDefault("threadNum", 8);
			double sampleRatioPerPartition = params.get(LineParams.SAMPLE_RATIO_PER_PARTITION);
			Thread[] thread = new Thread[threadNum];

			//save the vertices which need to be pulled.
			Set <Long>[] output = new Set[threadNum];

			DistributedInfo distributedInfo = new DefaultDistributedInfo();
			for (int i = 0; i < threadNum; ++i) {
				int start = (int) distributedInfo.startPos(i, threadNum, data.size());
				int end = (int) distributedInfo.localRowCnt(i, threadNum, data.size()) + start;
				LOG.info("taskId: {}, negStart: {}, end: {}", getPatitionId(), start, end);
				output[i] = new HashSet <>();
				thread[i] = new NegSampleRunner(nsPool, seed + i, data.subList(start, end), output[i], negaTime,
					sampleRatioPerPartition);
				thread[i].start();
			}

			for (int i = 0; i < threadNum; ++i) {
				thread[i].join();
			}

			Set <Long> outputMerger = new HashSet <>();

			for (int i = 0; i < threadNum; ++i) {
				outputMerger.addAll(output[i]);
			}

			LOG.info("taskId: {}, negOutputSize: {}", getPatitionId(), outputMerger.size());

			long endTimeConsumption = System.nanoTime();

			LOG.info("taskId: {}, negTime: {}", getPatitionId(),
				(endTimeConsumption - startTimeConsumption) / 1000000.0);

			return outputMerger;
		} else {
			throw new RuntimeException();
		}
	}

	protected static class NegSampleRunner extends Thread {
		private final Long[] nsPool;
		List <Number[]> input;
		Set <Long> output;
		int seed;
		int negaTime;
		double sampleRatioPerPartition;

		NegSampleRunner(Long[] nsPool, int seed, List <Number[]> input, Set <Long> output, int negaTime,
						double sampleRatioPerPartition) {
			this.nsPool = nsPool;
			this.input = input;
			this.output = output;
			this.seed = seed;
			this.negaTime = negaTime;
			this.sampleRatioPerPartition = sampleRatioPerPartition;
		}

		@Override
		public void run() {
			LinePullAndTrainOperation ns = new LinePullAndTrainOperation(negaTime, nsPool);
			ns.getIndexes(seed, sampleRatioPerPartition, input, output);
		}
	}
}
