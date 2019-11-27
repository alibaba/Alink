package com.alibaba.alink.operator.common.tree.parallelcart;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

public class InitialTrainningBuffer extends ComputeFunction {
	private static final Logger LOG = LoggerFactory.getLogger(InitialTrainningBuffer.class);
	private int featureColNum;
	private int algoType;
	private double subSamplingRatio;
	private double featureSubSamplingRatio;
	private Random random;

	public InitialTrainningBuffer(
		int algoType,
		int featureColNum,
		double subSamplingRatio,
		double featureSubSamplingRatio,
		int seed) {
		this.algoType = algoType;
		this.featureColNum = featureColNum;
		this.subSamplingRatio = subSamplingRatio;
		this.featureSubSamplingRatio = featureSubSamplingRatio;

		random = new Random(seed);
	}

	@Override
	public void calc(ComContext context) {
		if (context.getStepNo() > 1) {
			return;
		}

		LOG.info(Thread.currentThread().getName(), "open");

		//init dataNum
		//notice, this dataNum is an upper bound and could be inaccurate
		//needs to correct later
		int dataNum = 2;

		//we need byte buffers for: (dynamic) node_id, prediction
		// and (static) label, end_data_id(for ltr)
		byte[] nodeIdBuf = new byte[dataNum];
		float[] predBuf = new float[dataNum];
		byte[] curHashArray;
		if (algoType == 0 || algoType == 1) {
			curHashArray = new byte[dataNum * 4];
		} else {
			curHashArray = new byte[dataNum * 8];
		}
		ByteBuffer buf = ByteBuffer.wrap(curHashArray);

		//need to obtain data size within loop later
		int maxDataNum = 0;

		Iterable<Tuple2<Long, List<PartitionedData>>> inputRaw = context.getObj("trainData");

		if (inputRaw == null) {
			return;
		}
		Long F0 = 0L;

		int curIndex = 0;
		if (algoType == 0) {
			//regression
			for (Tuple2<Long, List<PartitionedData>> input : inputRaw) {
				F0 = input.f0;
				for (PartitionedData value2 : input.f1) {
					//hash array
					//order: node_id, prediction, gradient, hessian
					PartitionedData curData = value2;

					if ((curIndex + 2) * 4 >= curHashArray.length) {
						byte[] tCurHashArray = new byte[curHashArray.length * 2];
						System.arraycopy(curHashArray, 0, tCurHashArray, 0, curHashArray.length);
						curHashArray = tCurHashArray;
						buf = ByteBuffer.wrap(curHashArray);
					}
					if (curIndex >= nodeIdBuf.length) {
						byte[] tNodeIdBuf = new byte[nodeIdBuf.length * 2];
						System.arraycopy(nodeIdBuf, 0, tNodeIdBuf, 0, nodeIdBuf.length);
						nodeIdBuf = tNodeIdBuf;
					}
					if (curIndex >= predBuf.length) {
						float[] tPredBuf = new float[predBuf.length * 2];
						System.arraycopy(predBuf, 0, tPredBuf, 0, predBuf.length);
						predBuf = tPredBuf;
					}

					//sub sampling
					if (random.nextDouble() < subSamplingRatio) {
						nodeIdBuf[curIndex] = 0;
					} else {
						nodeIdBuf[curIndex] = -1;
					}

					predBuf[curIndex] = (float) 0.0;
					buf.putFloat(curIndex * 4, (float) curData.label);

					if (curIndex > maxDataNum) {
						maxDataNum = curIndex;
					}

					curIndex++;
				}
			}
		} else if (algoType == 1) {
			//binary classification
			for (Tuple2<Long, List<PartitionedData>> input : inputRaw) {
				F0 = input.f0;
				for (PartitionedData value2 : input.f1) {
					//hash array
					//order: node_id, prediction, gradient, hessian
					PartitionedData curData = value2;

					if ((curIndex + 2) * 4 >= curHashArray.length) {
						byte[] tCurHashArray = new byte[curHashArray.length * 2];
						System.arraycopy(curHashArray, 0, tCurHashArray, 0, curHashArray.length);
						curHashArray = tCurHashArray;
						buf = ByteBuffer.wrap(curHashArray);
					}
					if (curIndex >= nodeIdBuf.length) {
						byte[] tNodeIdBuf = new byte[nodeIdBuf.length * 2];
						System.arraycopy(nodeIdBuf, 0, tNodeIdBuf, 0, nodeIdBuf.length);
						nodeIdBuf = tNodeIdBuf;
					}
					if (curIndex >= predBuf.length) {
						float[] tPredBuf = new float[predBuf.length * 2];
						System.arraycopy(predBuf, 0, tPredBuf, 0, predBuf.length);
						predBuf = tPredBuf;
					}

					//sub sampling
					if (random.nextDouble() < subSamplingRatio) {
						nodeIdBuf[curIndex] = 0;
					} else {
						nodeIdBuf[curIndex] = -1;
					}

					predBuf[curIndex] = (float) 0.0;
					buf.putFloat(curIndex * 4, (float) curData.label);

					if (curIndex > maxDataNum) {
						maxDataNum = curIndex;
					}

					curIndex++;
				}
			}
		} else if (algoType == 2 || algoType == 3 || algoType == 4) {
			//learning to rank
			//note: need to fill in "id" here

			int lastGroup = -1;
			int curGroupStart = 0;
			for (Tuple2<Long, List<PartitionedData>> input : inputRaw) {
				F0 = input.f0;
				for (PartitionedData value2 : input.f1) {

					assert (F0 == value2.getGroup());

					if ((curIndex + 2) * 8 >= curHashArray.length) {
						byte[] tCurHashArray = new byte[curHashArray.length * 2];
						System.arraycopy(curHashArray, 0, tCurHashArray, 0, curHashArray.length);
						curHashArray = tCurHashArray;
						buf = ByteBuffer.wrap(curHashArray);
					}
					if (curIndex >= nodeIdBuf.length) {
						byte[] tNodeIdBuf = new byte[nodeIdBuf.length * 2];
						System.arraycopy(nodeIdBuf, 0, tNodeIdBuf, 0, nodeIdBuf.length);
						nodeIdBuf = tNodeIdBuf;
					}
					if (curIndex >= predBuf.length) {
						float[] tPredBuf = new float[predBuf.length * 2];
						System.arraycopy(predBuf, 0, tPredBuf, 0, predBuf.length);
						predBuf = tPredBuf;
					}

					//hash array
					//order: node_id, label, prediction, end_id
					PartitionedData curData = value2;
					int curGroup = curData.getGroup();

					//sub sampling
					if (curGroup == lastGroup) {
						//use sampling result from the group
						nodeIdBuf[curIndex] = nodeIdBuf[curIndex - 1];
					} else {
						//sampling for new group
						if (random.nextDouble() < subSamplingRatio) {
							nodeIdBuf[curIndex] = 0;
						} else {
							nodeIdBuf[curIndex] = -1;
						}
					}

					if (lastGroup != -1 && curGroup != lastGroup) {
						//update end_id for last group
						for (int index = curGroupStart; index < curIndex; index++) {
							buf.putInt(index * 8 + 4, curIndex);
						}
						curGroupStart = curIndex;
						lastGroup = curGroup;
					}
					if (lastGroup == -1) {
						lastGroup = curGroup;
					}

					predBuf[curIndex] = (float) 0.0;

					//label=2^(raw_label)-1
					//max==31(avoid integer overflow, although we don't use integer here)
					buf.putFloat(curIndex * 8,
						(float) (Math.pow(2.0, curData.label > 31 ? 31 : curData.label) - 1.0));

					if (curIndex > maxDataNum) {
						maxDataNum = curIndex;
					}
					curIndex++;
				}
			}

			//end_id for final group
			for (int index = curGroupStart; index < curIndex; index++) {
				buf.putInt(index * 8 + 4, curIndex);
			}
		} else {
			assert (false);
		}

		//update (real) dataNum
		dataNum = maxDataNum + 1;
		//byte[] realHashArray = Arrays.copyOfRange(curHashArray, 0, dataNum * 16);

		//new, add feature byte
		//this indicates where this feature can be ignored(by approx.), will be used later
		//on initialization, set this as feature sub sampling set

		byte[] featureCutArray = new byte[featureColNum];
		for (int index = 0; index < featureColNum; index++) {
			//feature sub sampling
			if (random.nextDouble() < featureSubSamplingRatio) {
				featureCutArray[index] = 0;
			} else {
				//set to 2 (means feature subsampling)
				//1 means by upper/lower bound
				featureCutArray[index] = 2;
			}
		}

		context.putObj("nodeIdBuf", nodeIdBuf);
		context.putObj("predBuf", predBuf);
		context.putObj("trainDetailArray", curHashArray);
		context.putObj("featureCutArray", featureCutArray);

		LOG.info(Thread.currentThread().getName(), "close");
	}

}
