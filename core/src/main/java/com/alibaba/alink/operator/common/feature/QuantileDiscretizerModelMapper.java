package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.operator.common.tree.Preprocessing;
import com.alibaba.alink.params.dataproc.HasHandleInvalid;
import com.alibaba.alink.params.feature.HasEncodeWithoutWoe;
import com.alibaba.alink.params.feature.QuantileDiscretizerPredictParams;
import com.alibaba.alink.params.shared.colname.HasOutputCol;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.alibaba.alink.params.feature.HasEncodeWithoutWoe.Encode.ASSEMBLED_VECTOR;

/**
 * quantile discretizer model data mapper.
 */
public class QuantileDiscretizerModelMapper extends ModelMapper implements Cloneable {
	private static final long serialVersionUID = 5400967430347827818L;
	private DiscreteMapperBuilder mapperBuilder;

	public QuantileDiscretizerModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		QuantileDiscretizerModelDataConverter model = new QuantileDiscretizerModelDataConverter();
		model.load(modelRows);

		mapperBuilder = new DiscreteMapperBuilder(params, getDataSchema());

		for (int i = 0; i < mapperBuilder.paramsBuilder.selectedCols.length; i++) {
			ContinuousRanges featureInterval = model.data.get(mapperBuilder.paramsBuilder.selectedCols[i]);
			Preconditions.checkNotNull(featureInterval, "%s not found in model",
				mapperBuilder.paramsBuilder.selectedCols[i]);
			long maxIndex = (long) featureInterval.getIntervalNum() - 1;

			long maxIndexWithNull = featureInterval.getIntervalNum();

			switch (mapperBuilder.paramsBuilder.handleInvalidStrategy) {
				case KEEP:
					mapperBuilder.vectorSize.put(i, maxIndexWithNull + 1);
					break;
				case SKIP:
				case ERROR:
					mapperBuilder.vectorSize.put(i, maxIndex + 1);
					break;
				default:
					throw new AkUnsupportedOperationException("Unsupported now.");
			}

			if (mapperBuilder.paramsBuilder.dropLast) {
				mapperBuilder.dropIndex.put(i, maxIndex);
			}

			mapperBuilder.discretizers[i] = createQuantileDiscretizer(featureInterval, model.meta);
		}

		mapperBuilder.setAssembledVectorSize();
		mapperBuilder.open();
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema modelSchema, TableSchema dataSchema, Params params) {

		DiscreteMapperBuilder mapperBuilder = new DiscreteMapperBuilder(params, getDataSchema());

		return Tuple4.of(
			mapperBuilder.paramsBuilder.selectedCols,
			mapperBuilder.paramsBuilder.resultCols,
			mapperBuilder.paramsBuilder.resultColTypes,
			mapperBuilder.paramsBuilder.reservedCols
		);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		mapperBuilder.map(selection, result);
	}

	public static class DiscreteMapperBuilder implements Serializable, Cloneable {
		private static final long serialVersionUID = -1726998479492235578L;
		DiscreteParamsBuilder paramsBuilder;
		Map <Integer, Long> vectorSize;
		Map <Integer, Long> dropIndex;
		Integer assembledVectorSize;
		NumericQuantileDiscretizer[] discretizers;
		transient ThreadLocal <Long[]> predictIndices;

		public DiscreteMapperBuilder(Params params, TableSchema dataSchema) {
			paramsBuilder = new DiscreteParamsBuilder(params, dataSchema,
				params.get(QuantileDiscretizerPredictParams.ENCODE));
			vectorSize = new HashMap <>();
			dropIndex = new HashMap <>();
			discretizers = new NumericQuantileDiscretizer[paramsBuilder.selectedCols.length];
		}

		void open() {
			predictIndices = ThreadLocal.withInitial(() -> new Long[paramsBuilder.selectedCols.length]);
		}

		void setAssembledVectorSize() {
			assembledVectorSize = vectorSize.values().stream().mapToInt(Long::intValue).sum();
			if (paramsBuilder.dropLast) {
				assembledVectorSize -= vectorSize.size();
			}

		}

		void map(SlicedSelectedSample selection, SlicedResult result) {
			Long[] predictIndices2 = this.predictIndices.get();
			for (int i = 0; i < paramsBuilder.selectedCols.length; i++) {
				Object val = selection.get(i);
				int foundIndex = discretizers[i].findIndex(val);
				predictIndices2[i] = (long) foundIndex;
				if (!discretizers[i].isValid(foundIndex)) {
					switch (paramsBuilder.handleInvalidStrategy) {
						case KEEP:
							break;
						case SKIP:
							predictIndices2[i] = null;
							break;
						case ERROR:
							throw new AkIllegalDataException("Unseen token: " + val);
						default:
							throw new AkIllegalOperatorParameterException("Invalid handle invalid strategy.");
					}
				}
			}

			Row res =
				setResultRow(
					predictIndices2,
					paramsBuilder.encode,
					dropIndex,
					vectorSize,
					paramsBuilder.dropLast,
					assembledVectorSize);
			for (int i = 0; i < res.getArity(); i++) {
				result.set(i, res.getField(i));
			}
		}

	}

	public static class DiscreteParamsBuilder implements Serializable {
		private static final long serialVersionUID = 8218203038244120910L;
		public HasEncodeWithoutWoe.Encode encode;
		public HasHandleInvalid.HandleInvalid handleInvalidStrategy;
		public String[] selectedCols;
		public String[] resultCols;
		public TypeInformation <?>[] resultColTypes;
		public String[] reservedCols;
		public boolean dropLast;

		public DiscreteParamsBuilder(Params params, TableSchema dataSchema, HasEncodeWithoutWoe.Encode encode) {
			reservedCols = params.get(QuantileDiscretizerPredictParams.RESERVED_COLS);
			handleInvalidStrategy = params.get(QuantileDiscretizerPredictParams.HANDLE_INVALID);
			this.encode = encode;

			//To delete when open source, to be compatible with previous versions
			if (!params.contains(QuantileDiscretizerPredictParams.OUTPUT_COLS) && params.contains(
				HasOutputCol.OUTPUT_COL)) {
				params.set(QuantileDiscretizerPredictParams.OUTPUT_COLS,
					new String[] {params.get(HasOutputCol.OUTPUT_COL)});
			}
			if (!params.contains(QuantileDiscretizerPredictParams.SELECTED_COLS)) {
				Preconditions.checkArgument(
					encode.equals(ASSEMBLED_VECTOR),
					"Not given selectedCols, encode must be ASSEMBLED_VECTOR!"
				);
			} else {
				selectedCols = params.get(QuantileDiscretizerPredictParams.SELECTED_COLS);
			}

			resultCols = params.get(QuantileDiscretizerPredictParams.OUTPUT_COLS);

			switch (encode) {
				case INDEX: {
					if (null == resultCols) {
						resultCols = selectedCols;
					}
					Preconditions.checkArgument(resultCols.length == selectedCols.length,
						"Input column name is not match output column name.");
					resultColTypes = new TypeInformation[resultCols.length];
					Arrays.fill(resultColTypes, Types.LONG);
					break;
				}
				case VECTOR: {
					if (null == resultCols) {
						resultCols = selectedCols;
					}
					Preconditions.checkArgument(resultCols.length == selectedCols.length,
						"Input column name is not match output column name.");
					resultColTypes = new TypeInformation[resultCols.length];
					Arrays.fill(resultColTypes, AlinkTypes.SPARSE_VECTOR);
					break;
				}
				case ASSEMBLED_VECTOR: {
					String[] outputCols = params.get(QuantileDiscretizerPredictParams.OUTPUT_COLS);
					Preconditions.checkArgument(null != outputCols && outputCols.length == 1,
						"When encode is ASSEMBLED_VECTOR, outputCols must be given and the length must be 1!");
					resultColTypes = new TypeInformation[resultCols.length];
					Arrays.fill(resultColTypes, AlinkTypes.SPARSE_VECTOR);
					break;
				}
				default: {
					throw new AkIllegalOperatorParameterException("Not support encode: " + encode.name());
				}
			}
			dropLast = params.get(QuantileDiscretizerPredictParams.DROP_LAST);
		}
	}

	public interface NumericQuantileDiscretizer extends Serializable {
		boolean isValid(int index);

		int findIndex(Object number);
	}

	public static class DoubleNumericQuantileDiscretizer implements NumericQuantileDiscretizer {
		private static final long serialVersionUID = -1681225445245237307L;
		double[] bounds;
		boolean isLeftOpen;
		int[] boundIndex;
		int nullIndex;
		boolean zeroAsMissing;

		public DoubleNumericQuantileDiscretizer(
			double[] bounds, boolean isLeftOpen, int[] boundIndex, int nullIndex, boolean zeroAsMissing) {
			this.bounds = bounds;
			this.isLeftOpen = isLeftOpen;
			this.boundIndex = boundIndex;
			this.nullIndex = nullIndex;
			this.zeroAsMissing = zeroAsMissing;
		}

		@Override
		public boolean isValid(int index) {
			return index != nullIndex;
		}

		@Override
		public int findIndex(Object number) {
			if (number == null) {
				return nullIndex;
			}

			double dVal = ((Number) number).doubleValue();

			if (Preprocessing.isMissing(dVal, zeroAsMissing)) {
				return nullIndex;
			}

			int hit = Arrays.binarySearch(bounds, dVal);

			if (isLeftOpen) {
				hit = hit >= 0 ? hit - 1 : -hit - 2;
			} else {
				hit = hit >= 0 ? hit : -hit - 2;
			}

			return boundIndex[hit];
		}
	}

	public static class LongQuantileDiscretizer implements NumericQuantileDiscretizer {
		private static final long serialVersionUID = 8869074090757935247L;
		long[] bounds;
		boolean isLeftOpen;
		int[] boundIndex;
		int nullIndex;
		boolean zeroAsMissing;

		public LongQuantileDiscretizer(
			long[] bounds, boolean isLeftOpen, int[] boundIndex, int nullIndex, boolean zeroAsMissing) {
			this.bounds = bounds;
			this.isLeftOpen = isLeftOpen;
			this.boundIndex = boundIndex;
			this.nullIndex = nullIndex;
			this.zeroAsMissing = zeroAsMissing;
		}

		@Override
		public boolean isValid(int index) {
			return index != nullIndex;
		}

		@Override
		public int findIndex(Object number) {
			if (number == null) {
				return nullIndex;
			}

			long lVal = ((Number) number).longValue();

			if (Preprocessing.isMissing(lVal, zeroAsMissing)) {
				return nullIndex;
			}

			int hit = Arrays.binarySearch(bounds, lVal);

			if (isLeftOpen) {
				hit = hit >= 0 ? hit - 1 : -hit - 2;
			} else {
				hit = hit >= 0 ? hit : -hit - 2;
			}

			return boundIndex[hit];
		}
	}

	public static NumericQuantileDiscretizer createQuantileDiscretizer(
		ContinuousRanges featureBorder, Params params) {
		int size = featureBorder.splitsArray.length + 1;
		boolean isLeftOpen = featureBorder.getLeftOpen();
		int nullIndex = featureBorder.getIntervalNum();

		int[] boundIndex = IntStream.range(0, size + 2).toArray();
		boundIndex[size] = size - 1;

		if (!featureBorder.isFloat()) {
			long[] bounds = new long[size + 1];
			bounds[0] = -Long.MAX_VALUE;
			for (int i = 0; i < size - 1; ++i) {
				bounds[i + 1] = featureBorder.splitsArray[i].longValue();
			}
			bounds[size] = Long.MAX_VALUE;
			return new LongQuantileDiscretizer(
				bounds, isLeftOpen, boundIndex, nullIndex,
				params.get(Preprocessing.ZERO_AS_MISSING));
		} else {
			double[] bounds = new double[size + 1];
			bounds[0] = Double.NEGATIVE_INFINITY;
			for (int i = 0; i < size - 1; ++i) {
				bounds[i + 1] = featureBorder.splitsArray[i].doubleValue();
			}
			bounds[size] = Double.POSITIVE_INFINITY;

			return new DoubleNumericQuantileDiscretizer(
				bounds, isLeftOpen, boundIndex, nullIndex,
				params.get(Preprocessing.ZERO_AS_MISSING)
			);
		}
	}

	/**
	 * Set the result row when predict index is gotten.
	 *
	 * @param predictIndices      predict indices.
	 * @param encode              Encode type.
	 * @param dropIndex           If dropLast is true, this index is drop.
	 * @param vectorSize          The vectorsizes of each column.
	 * @param dropLast            Drop the last token or not.
	 * @param assembledVectorSize If encode is ASSEMBLED_VECTOR, this size is used.
	 * @return the result row.
	 */
	public static Row setResultRow(Long[] predictIndices,
								   HasEncodeWithoutWoe.Encode encode,
								   Map <Integer, Long> dropIndex,
								   Map <Integer, Long> vectorSize,
								   boolean dropLast,
								   int assembledVectorSize) {
		Row result = null;
		int[] calcResultColIndices = null;
		switch (encode) {
			case INDEX:
			case VECTOR:
				result = new Row(predictIndices.length);
				calcResultColIndices = new int[predictIndices.length];
				for (int i = 0; i < predictIndices.length; i++) {
					calcResultColIndices[i] = i;
				}
				break;
			case ASSEMBLED_VECTOR:
				result = new Row(1);
				calcResultColIndices = new int[] {0};
				break;
			default:
				throw new AkUnsupportedOperationException("Not support encode type!");
		}
		setResultRow(predictIndices, encode, dropIndex, vectorSize,
			dropLast, assembledVectorSize, result, calcResultColIndices);
		return result;
	}

	public static void setResultRow(Long[] predictIndices,
									HasEncodeWithoutWoe.Encode encode,
									Map <Integer, Long> dropIndex,
									Map <Integer, Long> vectorSize,
									boolean dropLast,
									int assembledVectorSize,
									Row rowBuffer,
									int[] calcResultColIndices) {
		switch (encode) {
			case INDEX: {
				for (int i = 0; i < calcResultColIndices.length; i++) {
					rowBuffer.setField(calcResultColIndices[i], predictIndices[i]);
				}
				break;
			}
			case VECTOR: {
				for (int i = 0; i < calcResultColIndices.length; i++) {
					if (predictIndices[i] == null) {
						rowBuffer.setField(calcResultColIndices[i], null);
					} else {
						Tuple2 <Integer, Integer> tuple = getVectorSizeAndIndex(predictIndices[i], dropIndex.get(i),
							vectorSize.get(i), dropLast);
						rowBuffer.setField(calcResultColIndices[i], null == tuple.f1 ? new SparseVector(tuple.f0)
							: new SparseVector(tuple.f0, new int[] {tuple.f1}, new double[] {1.0}));
					}
				}
				break;
			}
			case ASSEMBLED_VECTOR: {
				List <Integer> list = new ArrayList <>();
				int startIndex = 0;
				for (int i = 0; i < predictIndices.length; i++) {
					if (null == predictIndices[i]) {
						rowBuffer.setField(calcResultColIndices[i], null);
					}
					Tuple2 <Integer, Integer> tuple = getVectorSizeAndIndex(predictIndices[i], dropIndex.get(i),
						vectorSize.get(i), dropLast);
					if (tuple.f1 != null) {
						list.add(startIndex + tuple.f1);
					}
					startIndex += tuple.f0;
				}
				double[] values = new double[list.size()];
				Arrays.fill(values, 1.0);
				int[] indices = new int[list.size()];
				for (int i = 0; i < list.size(); i++) {
					indices[i] = list.get(i);
				}
				rowBuffer.setField(calcResultColIndices[0], new SparseVector(assembledVectorSize, indices, values));
				break;
			}
			default: {
				throw new AkUnsupportedOperationException("Not support encode type!");
			}
		}
	}

	/**
	 * Return the vectorsize the final index.
	 *
	 * @param predictIndex     predict index.
	 * @param dropIndex        the index to drop, if dropLast is true, the return index is null.
	 * @param originVectorSize origin vectorsize.
	 * @param dropLast         Drop the last index or not.
	 * @return (vectorSize, index)
	 */
	private static Tuple2 <Integer, Integer> getVectorSizeAndIndex(Long predictIndex,
																   Long dropIndex,
																   Long originVectorSize,
																   boolean dropLast) {
		if (dropLast) {
			int vectorSize = originVectorSize.intValue() - 1;
			if (predictIndex.equals(dropIndex)) {
				return Tuple2.of(vectorSize, null);
			} else {
				return Tuple2.of(vectorSize,
					predictIndex > dropIndex ? predictIndex.intValue() - 1 : predictIndex.intValue());
			}
		} else {
			return Tuple2.of(originVectorSize.intValue(), predictIndex.intValue());
		}
	}

}
