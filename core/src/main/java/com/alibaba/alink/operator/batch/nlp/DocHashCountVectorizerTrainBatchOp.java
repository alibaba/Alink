package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.shaded.guava18.com.google.common.hash.HashFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.nlp.DocHashCountVectorizerModelData;
import com.alibaba.alink.operator.common.nlp.DocHashCountVectorizerModelDataConverter;
import com.alibaba.alink.operator.common.nlp.NLPConstant;
import com.alibaba.alink.params.nlp.DocHashCountVectorizerTrainParams;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.flink.shaded.guava18.com.google.common.hash.Hashing.murmur3_32;

/**
 * Hash every word as a number, and save the Inverse document frequency(IDF) of every word in the document.
 * <p>
 * It's used together with DocHashCountVectorizerPredictBatchOp.
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.MODEL)})
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("文本哈希特征生成训练")
public class DocHashCountVectorizerTrainBatchOp extends BatchOperator <DocHashCountVectorizerTrainBatchOp>
	implements DocHashCountVectorizerTrainParams <DocHashCountVectorizerTrainBatchOp> {
	private static final long serialVersionUID = 6469196128919853279L;

	public DocHashCountVectorizerTrainBatchOp() {
		super(new Params());
	}

	public DocHashCountVectorizerTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public DocHashCountVectorizerTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		int index = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), this.getSelectedCol());

		DataSet <Row> out = in
			.getDataSet()
			.rebalance()
			.mapPartition(new HashingTF(index, this.getNumFeatures()))
			.reduce(new ReduceFunction <Tuple2 <Long, HashMap <Integer, Double>>>() {
				private static final long serialVersionUID = 7849950640425941402L;

				@Override
				public Tuple2 <Long, HashMap <Integer, Double>> reduce(Tuple2 <Long, HashMap <Integer, Double>> map1,
																	   Tuple2 <Long, HashMap <Integer, Double>> map2) {
					map2.f1.forEach((k2, v1) -> map1.f1.merge(k2, v1, Double::sum));
					map1.f0 += map2.f0;
					return map1;
				}
			}).flatMap(new BuildModel(getParams())).setParallelism(1);
		this.setOutput(out, new DocHashCountVectorizerModelDataConverter().getModelSchema());

		return this;
	}

	/**
	 * The dense vector contains the document frequency(DF) of words. Calculate the IDF, and replace the original DF
	 * value. The dense vector is saved into DocHashIDFVectorizerModel.
	 */
	static class BuildModel implements FlatMapFunction <Tuple2 <Long, HashMap <Integer, Double>>, Row> {
		private static final long serialVersionUID = 5598329225247209656L;
		private double minDocFrequency;
		private int numFeatures;
		private String featureType;
		private double minTF;

		public BuildModel(Params params) {
			this.minDocFrequency = params.get(DocHashCountVectorizerTrainParams.MIN_DF);
			this.numFeatures = params.get(DocHashCountVectorizerTrainParams.NUM_FEATURES);
			this.featureType = params.get(DocHashCountVectorizerTrainParams.FEATURE_TYPE).name();
			this.minTF = params.get(DocHashCountVectorizerTrainParams.MIN_TF);
		}

		@Override
		public void flatMap(Tuple2 <Long, HashMap <Integer, Double>> vec, Collector <Row> collector) throws Exception {
			long cnt = vec.f0;
			minDocFrequency = minDocFrequency >= 1.0 ? minDocFrequency : minDocFrequency * cnt;
			Iterator <Map.Entry <Integer, Double>> it = vec.f1.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry <Integer, Double> entry = it.next();
				if (entry.getValue() >= minDocFrequency) {
					entry.setValue(Math.log((cnt + 1.0) / (entry.getValue() + 1.0)));
				} else {
					it.remove();
				}
			}
			DocHashCountVectorizerModelData model = new DocHashCountVectorizerModelData();
			model.numFeatures = numFeatures;
			model.minTF = minTF;
			model.featureType = featureType;
			model.idfMap = vec.f1;
			new DocHashCountVectorizerModelDataConverter().save(model, collector);
		}
	}

	/**
	 * Transform a word to a number by using MurMurHash3. The number is used as the index of a dense vector. The value
	 * of the vector is document frequency of the corresponding word.
	 */
	static class HashingTF implements MapPartitionFunction <Row, Tuple2 <Long, HashMap <Integer, Double>>> {
		private static final long serialVersionUID = -7172651314711810032L;
		private int index, numFeatures;
		private static final HashFunction HASH = murmur3_32(0);

		public HashingTF(int index, int numFeatures) {
			this.index = index;
			this.numFeatures = numFeatures;
		}

		@Override
		public void mapPartition(Iterable <Row> iterable,
								 Collector <Tuple2 <Long, HashMap <Integer, Double>>> collector)
			throws Exception {
			HashMap <Integer, Double> map = new HashMap <>(numFeatures);
			long count = 0;
			for (Row row : iterable) {
				count++;
				String content = (String) row.getField(index);
				String[] words = content.split(NLPConstant.WORD_DELIMITER);

				for (String word : words) {
					int hashValue = Math.abs(HASH.hashUnencodedChars(word).asInt());
					int index = Math.floorMod(hashValue, numFeatures);
					map.merge(index, 1.0, Double::sum);
				}
			}
			collector.collect(Tuple2.of(count, map));
		}
	}
}
