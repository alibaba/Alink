package com.alibaba.alink.operator.common.tree.parallelcart;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DataFormatToArray extends ComputeFunction {
	private static final Logger LOG = LoggerFactory.getLogger(DataFormatToArray.class);
	private int featureColNum;

	public DataFormatToArray(int featureColNum) {
		this.featureColNum = featureColNum;
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

		//initialize input_features

		//need to obtain data size within loop later
		int maxDataNum = 0;
		byte[] inputFeatures = new byte[featureColNum * dataNum];

		Iterable <Tuple2 <Long, List <PartitionedData>>> inputRaw = context.getObj("trainData");

		if (inputRaw==null) {
			return;
		}

		Long F0 = 0L;
		int curIndex = 0;

		for (Tuple2 <Long, List <PartitionedData>> input : inputRaw) {
			F0 = input.f0;
			for (PartitionedData value2 : input.f1) {
				//get features
				PartitionedData curData = value2;

				if (curIndex > maxDataNum) {
					maxDataNum = curIndex;
				}
				int curIndexForFeature = curIndex * featureColNum;
				for (int colIndex = 0; colIndex < featureColNum; colIndex++) {
					if (curIndexForFeature >= inputFeatures.length) {
						byte[] tInputFeatures = new byte[inputFeatures.length * 3 / 2];
						System.arraycopy(inputFeatures, 0, tInputFeatures, 0, inputFeatures.length);
						inputFeatures = tInputFeatures;
					}
					byte fea = curData.features[colIndex];
					inputFeatures[curIndexForFeature] = fea;
					curIndexForFeature++;
				}
				curIndex++;
			}
		}

		//update (real) dataNum
		dataNum = maxDataNum + 1;

		//brute force transform the data
		//this provides better locality during "construct bin"
		//todo: improve the brute force transform
		byte[] realInputFeatures = new byte[featureColNum * dataNum];
		for (int index = 0; index < dataNum; index++) {
			for (int featureIndex = 0; featureIndex < featureColNum; featureIndex++) {
				realInputFeatures[featureIndex * dataNum + index] = inputFeatures[featureIndex + index *
					featureColNum];
			}
		}

		context.putObj("featuresArray", realInputFeatures);
		context.removeObj("trainData");

		LOG.info(Thread.currentThread().getName(), "close");
	}
}