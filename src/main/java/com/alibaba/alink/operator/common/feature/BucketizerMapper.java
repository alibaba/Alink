package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.feature.BucketizerParams;

import java.util.Arrays;

/**
 * Map a continuous variable into several buckets. The range of a bucket is [x,y) except the last bucket, which also
 * includes y.
 *
 * <p>It supports a single column input or multiple columns input. The lengths of selectedCols, outputCols and
 * splitsArrays should be equal. In the case of multiple columns, each column used the corresponding splits.
 *
 * <p>Split array must be strictly increasing and have at least three points.
 */
public class BucketizerMapper extends Mapper {
	private static final int MIN_SPLIT_POINT_NUM = 3;
	private static final String SPLIT_DELIMITER = ":";

	private enum HandleType {
		/**
		 * When encounter an invalid data, throw IllegalArgumentException.
		 */
		ERROR,
		/**
		 * When encounter an invalid data, keep invalid values in a special bucket(split.length - 1).
		 */
		KEEP
	}

	private int[] inputColIndexes;
	private OutputColsHelper outputColsHelper;
	private double[][] splitsArray;
	private HandleType handleInvalid;

	public BucketizerMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		String[] selectedCols = params.get(BucketizerParams.SELECTED_COLS);
		String[] outputCols = params.get(BucketizerParams.OUTPUT_COLS);
		String[] splitsStrArray = params.get(BucketizerParams.SPLITS_ARRAY);
		splitsArray = new double[splitsStrArray.length][];
		for(int i = 0; i < splitsStrArray.length; i++){
			splitsArray[i] = parseString(splitsStrArray[i]);
		}
		handleInvalid = HandleType.valueOf(params.get(BucketizerParams.HANDLE_INVALID).toUpperCase());
		inputColIndexes = TableUtil.findColIndices(dataSchema.getFieldNames(), selectedCols);

		if (null == outputCols) {
			outputCols = selectedCols;
		}

		if (selectedCols.length != outputCols.length || selectedCols.length != splitsArray.length) {
			throw new IllegalArgumentException("The length of inputs are not equal!");
		}

		TypeInformation[] outputColTypes = new TypeInformation[selectedCols.length];
		Arrays.fill(outputColTypes, Types.INT);
		outputColsHelper = new OutputColsHelper(
			dataSchema,
			outputCols,
			outputColTypes,
			this.params.get(BucketizerParams.RESERVED_COLS)
		);
	}

	@Override
	public TableSchema getOutputSchema() {
		return outputColsHelper.getResultSchema();
	}

	/**
	 * Map a continuous variable into several buckets.
	 *
	 * @param row the input Row type data
	 * @return the output Row
	 */
	@Override
	public Row map(Row row) throws Exception {
		Row out = new Row(inputColIndexes.length);
		for (int i = 0; i < inputColIndexes.length; i++) {
			if (null == row.getField(inputColIndexes[i])) {
				out.setField(i, null);
			}
			double data = ((Number) row.getField(inputColIndexes[i])).doubleValue();
			if (Double.isNaN(data)) {
				switch (handleInvalid) {
					case ERROR: {
						throw new IllegalArgumentException("Bucketizer encountered NaN value.!");
					}
					case KEEP: {
						out.setField(i, splitsArray[i].length - 1);
						break;
					}
					default: {
						throw new IllegalArgumentException("Not Support!");
					}
				}
			} else {
				out.setField(i, bucketizer(data, splitsArray[i]));
			}
		}
		return outputColsHelper.getResultRow(row, out);
	}

	/**
	 * Binary search the target value from the buckets.
	 *
	 * @param value  target value
	 * @param splits bucket split array
	 * @return the bucket index
	 */
	private static int bucketizer(double value, double[] splits) {
		int res;
		int length = splits.length;
		if (value == splits[length - 1]) {
			res = length - 2;
		} else {
			int index = Arrays.binarySearch(splits, value);
			if (index >= 0) {
				res = index;
			} else {
				int position = -index - 1;
				if (position == 0 || position == length) {
					throw new IllegalArgumentException("Feature value " + value + " out of bound!");
				} else {
					res = position - 1;
				}
			}
		}
		return res;
	}

	private static double[] parseString(String s){
		String[] splitArray = s.split(SPLIT_DELIMITER);
		double[] splits = new double[splitArray.length];
		for(int i = 0; i < splitArray.length; i++){
			splits[i] = Double.parseDouble(splitArray[i]);
		}
		if (splits.length < MIN_SPLIT_POINT_NUM) {
			throw new IllegalArgumentException("Split points should be at least 3!");
		}
		for (int i = 1; i < splits.length; i++) {
			if (splits[i] <= splits[i - 1]) {
				throw new IllegalArgumentException("Split points should be strictly increasing!");
			}
		}
		return splits;
	}
}
