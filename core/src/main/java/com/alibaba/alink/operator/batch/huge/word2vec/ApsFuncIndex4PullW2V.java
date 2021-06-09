package com.alibaba.alink.operator.batch.huge.word2vec;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.io.directreader.DefaultDistributedInfo;
import com.alibaba.alink.common.io.directreader.DistributedInfo;
import com.alibaba.alink.operator.common.aps.ApsContext;
import com.alibaba.alink.operator.common.aps.ApsFuncIndex4Pull;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ApsFuncIndex4PullW2V extends ApsFuncIndex4Pull <int[]> {
	private final static Logger LOG = LoggerFactory.getLogger(ApsFuncIndex4PullW2V.class);
	private static final long serialVersionUID = -341644994350955260L;

	private final Params params;

	public ApsFuncIndex4PullW2V(Params params) {
		this.params = params;
	}

	@Override
	protected Set <Long> requestIndex(List <int[]> data) throws Exception {
		long startTimeConsumption = System.nanoTime();

		LOG.info("taskId: {}, localId: {}", getPatitionId(), getRuntimeContext().getIndexOfThisSubtask());
		LOG.info("taskId: {}, negInputSize: {}", getPatitionId(), data.size());

		if (null != this.contextParams) {
			Long[] seeds = this.contextParams.get(ApsContext.SEEDS);
			Long[] nsPool = this.contextParams.getLongArray("negBound");
			final boolean metapathMode = params.getBoolOrDefault("metapathMode", false);
			long[] groupIdxStarts = null;

			if (metapathMode) {
				groupIdxStarts = ArrayUtils.toPrimitive(this.contextParams.getLongArray("groupIdxes"));
			}

			int vocSize = this.contextParams.getLong("vocSize").intValue();
			long seed = seeds[getPatitionId()];

			int threadNum = params.getIntegerOrDefault("threadNum", 8);

			Thread[] thread = new Thread[threadNum];
			Set <Long>[] output = new Set[threadNum];

			DistributedInfo distributedInfo = new DefaultDistributedInfo();

			for (int i = 0; i < threadNum; ++i) {
				int start = (int) distributedInfo.startPos(i, threadNum, data.size());
				int end = (int) distributedInfo.localRowCnt(i, threadNum, data.size()) + start;
				LOG.info("taskId: {}, negStart: {}, end: {}", getPatitionId(), start, end);
				output[i] = new HashSet <>();
				thread[i] = new NegSampleRunner(vocSize, nsPool, params,
					seed + i, data.subList(start, end), output[i], null, groupIdxStarts);
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

	public static class NegSampleRunner extends Thread {
		Params params;
		List <int[]> input;
		Set <Long> output;
		long seed;
		private int vocSize;
		private Long[] nsPool;
		private Object[] groupIdxObjs;
		private long[] groupIdxStarts;

		public NegSampleRunner(
			int vocSize, Long[] nsPool, Params params,
			long seed,
			List <int[]> input,
			Set <Long> output, Object[] groupIdxObjs, long[] groupIdxStarts) {
			this.vocSize = vocSize;
			this.nsPool = nsPool;
			this.params = params;
			this.input = input;
			this.output = output;
			this.seed = seed;
			this.groupIdxObjs = groupIdxObjs;
			this.groupIdxStarts = groupIdxStarts;
		}

		@Override
		public void run() {
			Word2Vec w2v = new Word2Vec(this.params, this.vocSize, this.nsPool, groupIdxObjs, groupIdxStarts);
			w2v.getIndexes(seed, input, output);
		}
	}

}
