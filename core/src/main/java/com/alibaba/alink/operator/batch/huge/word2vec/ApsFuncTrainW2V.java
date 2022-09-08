package com.alibaba.alink.operator.batch.huge.word2vec;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.io.directreader.DefaultDistributedInfo;
import com.alibaba.alink.common.io.directreader.DistributedInfo;
import com.alibaba.alink.operator.common.aps.ApsFuncTrain;
import com.alibaba.alink.params.shared.HasVectorSizeDefaultAs100;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class ApsFuncTrainW2V extends ApsFuncTrain <int[], float[]> {
	private final static Logger LOG = LoggerFactory.getLogger(ApsFuncTrainW2V.class);
	private static final long serialVersionUID = -6458331525690591343L;

	private final Params params;

	public ApsFuncTrainW2V(Params params) {
		this.params = params;
	}

	@Override
	protected List <Tuple2 <Long, float[]>> train(List <Tuple2 <Long, float[]>> relatedFeatures,
												  Map <Long, Integer> mapFeatureId2Local, List <int[]> trainData)
		throws Exception {
		if (null != this.contextParams) {
			long startTimeConsumption = System.nanoTime();

			LOG.info("taskId: {}, localId: {}", getPatitionId(), getRuntimeContext().getIndexOfThisSubtask());

			Long[] seeds = this.contextParams.getLongArray("seeds");
			Long[] nsPool = this.contextParams.getLongArray("negBound");
			final boolean metapathMode = params.getBoolOrDefault("metapathMode", false);
			long[] groupIdxStarts = null;

			if (metapathMode) {
				groupIdxStarts = ArrayUtils.toPrimitive(this.contextParams.getLongArray("groupIdxes"));
			}

			int vocSize = this.contextParams.getLong("vocSize").intValue();
			long seed = seeds[getPatitionId()];

			LOG.info("taskId: {}, trainDataSize: {}", getPatitionId(), trainData.size());

			int vectorSize = params.get(HasVectorSizeDefaultAs100.VECTOR_SIZE);

			float[] buffer = new float[relatedFeatures.size() * vectorSize];
			int cur = 0;
			int inputSize = 0;
			for (Tuple2 <Long, float[]> val : relatedFeatures) {
				if (val.f0 < vocSize) {
					inputSize++;
				}

				System.arraycopy(val.f1, 0, buffer, cur * vectorSize, vectorSize);
				cur++;
			}

			LOG.info("taskId: {}, trainInputSize: {}", getPatitionId(), inputSize);
			LOG.info("taskId: {}, trainOutputSize: {}", getPatitionId(), relatedFeatures.size() - inputSize);

			new TrainSubSet(vocSize, nsPool, params, getPatitionId(), null, groupIdxStarts)
				.train(seed, trainData, buffer, mapFeatureId2Local);

			for (int i = 0; i < cur; ++i) {
				System.arraycopy(buffer, i * vectorSize, relatedFeatures.get(i).f1, 0, vectorSize);
			}

			long endTimeConsumption = System.nanoTime();

			LOG.info("taskId: {}, trainTime: {}", getPatitionId(),
				(endTimeConsumption - startTimeConsumption) / 1000000.0);

			return relatedFeatures;

		} else {
			throw new AkUnclassifiedErrorException("ApsFunction meets RuntimeException");
		}
	}

	public static class TrainSubSet {
		private int vocSize;
		private Long[] nsPool;
		private Object[] groupIdxObjs;
		private long[] groupIdxStarts;
		private Params params;
		private int taskId;

		public TrainSubSet(
			int vocSize, Long[] nsPool,
			Params params, int taskId, Object[] groupIdxObjs, long[] groupIdxStarts) {
			this.vocSize = vocSize;
			this.nsPool = nsPool;
			this.params = params;
			this.taskId = taskId;
			this.groupIdxObjs = groupIdxObjs;
			this.groupIdxStarts = groupIdxStarts;
		}

		public void train(
			long seed, List <int[]> data,
			float[] buffer, Map <Long, Integer> mapFeatureId2Local) throws InterruptedException {
			int threadNum = params.getIntegerOrDefault("threadNum", 8);

			Thread[] thread = new Thread[threadNum];

			DistributedInfo distributedInfo = new DefaultDistributedInfo();

			for (int i = 0; i < threadNum; ++i) {
				int start = (int) distributedInfo.startPos(i, threadNum, data.size());
				int end = (int) distributedInfo.localRowCnt(i, threadNum, data.size()) + start;
				LOG.info("taskId: {}, trainStart: {}, end: {}", taskId, start, end);
				thread[i] = new TrainSubSetRunner(vocSize, nsPool, params,
					seed + i, data.subList(start, end), buffer, mapFeatureId2Local, groupIdxObjs, groupIdxStarts);

				thread[i].start();
			}

			for (int i = 0; i < threadNum; ++i) {
				thread[i].join();
			}

		}

	}

	public static class TrainSubSetRunner extends Thread {
		Params params;
		List <int[]> input;
		long seed;
		Map <Long, Integer> mapFeatureId2Local;
		float[] buffer;
		private int vocSize;
		private Long[] nsPool;
		private Object[] groupIdxObjs;
		private long[] groupIdxStarts;

		public TrainSubSetRunner(
			int vocSize, Long[] nsPool, Params params,
			long seed,
			List <int[]> input,
			float[] buffer, Map <Long, Integer> mapFeatureId2Local, Object[] groupIdxObjs, long[] groupIdxStarts) {
			this.vocSize = vocSize;
			this.nsPool = nsPool;
			this.params = params;
			this.input = input;
			this.buffer = buffer;
			this.mapFeatureId2Local = mapFeatureId2Local;
			this.groupIdxObjs = groupIdxObjs;
			this.groupIdxStarts = groupIdxStarts;
			this.seed = seed;

		}

		@Override
		public void run() {
			Word2Vec w2v = new Word2Vec(this.params, this.vocSize, this.nsPool, groupIdxObjs, groupIdxStarts);
			w2v.train3(seed, input, buffer, mapFeatureId2Local);
		}
	}

}
