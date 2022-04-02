package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.shaded.guava18.com.google.common.hash.HashFunction;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.feature.FeatureHasherParams;
import org.apache.commons.lang3.ArrayUtils;

import java.util.TreeMap;

import static org.apache.flink.shaded.guava18.com.google.common.hash.Hashing.murmur3_32;

/**
 * Projects a number of categorical or numerical features into a feature vector of a specified dimension. It's done by
 * using MurMurHash3 to map the features to indices of the result vector and accumulate the corresponding value.
 *
 * <p>For categorical feature: the string to hash is "colName=value", here colName is the colName of the feature, the
 * value is the feature value. The corresponding value is set 1.0.
 *
 * <p>For numerical feature: the string to hash is "colName", the colName of the feature and use the numeric value as
 * the corresponding value.
 *
 * <p>The numerical feature and categorical feature can be determined automatically. You can also change the numerical
 * features to categorical features by determine the CATEGORICAL_COLS parameter.
 *
 * <p>(https://en.wikipedia.org/wiki/Feature_hashing)
 */
public class FeatureHasherMapper extends Mapper {
	private static final long serialVersionUID = -2866356757003004579L;

	private int[] numericColIndexes;
	private int[] categoricalColIndexes;
	private static final HashFunction HASH = murmur3_32(0);
	private int numFeature;
	private String[] selectedCols;

	public FeatureHasherMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		selectedCols = this.params.get(FeatureHasherParams.SELECTED_COLS);
		String[] categoricalCols = TableUtil.getCategoricalCols(
			dataSchema,
			selectedCols,
			params.contains(FeatureHasherParams.CATEGORICAL_COLS) ?
				params.get(FeatureHasherParams.CATEGORICAL_COLS) : null
		);
		String[] numericCols = ArrayUtils.removeElements(selectedCols, categoricalCols);

		numericColIndexes = TableUtil.findColIndicesWithAssertAndHint(selectedCols, numericCols);
		categoricalColIndexes = TableUtil.findColIndicesWithAssertAndHint(selectedCols, categoricalCols);

		numFeature = this.params.get(FeatureHasherParams.NUM_FEATURES);
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema dataSchema, Params params) {

		return Tuple4.of(this.params.get(FeatureHasherParams.SELECTED_COLS),
			new String[] {this.params.get(FeatureHasherParams.OUTPUT_COL)},
			new TypeInformation<?>[] {AlinkTypes.VECTOR},
			this.params.get(FeatureHasherParams.RESERVED_COLS)
		);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		TreeMap <Integer, Double> feature = new TreeMap <>();
		for (int key : numericColIndexes) {
			if (null != selection.get(key)) {
				double value = ((Number) selection.get(key)).doubleValue();
				String colName = selectedCols[key];
				updateMap(colName, value, feature, numFeature);
			}
		}
		for (int key : categoricalColIndexes) {
			if (null != selection.get(key)) {
				String colName = selectedCols[key];
				updateMap(colName + "=" + selection.get(key).toString(), 1.0, feature, numFeature);
			}
		}

		result.set(0, new SparseVector(numFeature, feature));
	}

	/**
	 * Update the treeMap which saves the key-value pair of the final vector, use the hash value of the string as key
	 * and the accumulate the corresponding value.
	 *
	 * @param s     the string to hash
	 * @param value the accumulated value
	 */
	private static void updateMap(String s, double value, TreeMap <Integer, Double> feature, int numFeature) {
		int hashValue = Math.abs(HASH.hashUnencodedChars(s).asInt());

		int index = Math.floorMod(hashValue, numFeature);
		if (feature.containsKey(index)) {
			feature.put(index, feature.get(index) + value);
		} else {
			feature.put(index, value);
		}
	}
}
