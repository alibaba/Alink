package com.alibaba.alink.operator.common.nlp;

import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.mapper.SISOModelMapper;
import com.alibaba.alink.common.VectorTypes;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.shaded.guava18.com.google.common.hash.HashFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.shaded.guava18.com.google.common.hash.Hashing.murmur3_32;

/**
 * Transform a document to a sparse vector based on the inverse document frequency(idf) statistics provided by
 * DocHashIDFVectorizerTrainBatchOp.
 *
 * <p>It uses MurmurHash 3 to get the hash value of a word as the index, and the idf of the word as value.
 */
public class DocHashCountVectorizerModelMapper extends SISOModelMapper {
	private DocHashCountVectorizerModelData model;
	private DocCountVectorizerModelMapper.FeatureType featureType;

	private static final HashFunction HASH = murmur3_32(0);

	public DocHashCountVectorizerModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		this.model = new DocHashCountVectorizerModelDataConverter().load(modelRows);
		this.featureType = DocCountVectorizerModelMapper.FeatureType.valueOf(model.featureType.toUpperCase());
	}

	@Override
	protected TypeInformation initPredResultColType() {
		return VectorTypes.SPARSE_VECTOR;
	}

	@Override
	protected Object predictResult(Object input) {
		if (null == input) {
			return null;
		}
		HashMap<Integer, Integer> wordCount = new HashMap<>(0);
		String content = (String) input;
		String[] tokens = content.split(NLPConstant.WORD_DELIMITER);
		double minTermCount = model.minTF >= 1.0 ? model.minTF : model.minTF * tokens.length;
		double tokenRatio = 1.0 / tokens.length;

		for (String token : tokens) {
			int hashValue = Math.abs(HASH.hashUnencodedChars(token).asInt());
			int index = Math.floorMod(hashValue, model.numFeatures);
			if(model.idfMap.containsKey(index)) {
				wordCount.merge(index, 1, Integer::sum);
			}
		}

		int[] indexes = new int[wordCount.size()];
		double[] values = new double[indexes.length];
		int pos = 0;
		for (Map.Entry<Integer, Integer> entry : wordCount.entrySet()) {
			double count = entry.getValue();
			if (count >= minTermCount) {
				indexes[pos] = entry.getKey();
				values[pos++] = featureType.featureValueFunc.apply(model.idfMap.get(entry.getKey()), count, tokenRatio);
			}
		}

		return new SparseVector(model.numFeatures, Arrays.copyOf(indexes, pos), Arrays.copyOf(values, pos));
	}
}
