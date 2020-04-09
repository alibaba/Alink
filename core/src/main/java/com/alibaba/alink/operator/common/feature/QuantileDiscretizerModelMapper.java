package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.feature.binning.Bin;
import com.alibaba.alink.operator.common.feature.binning.BinTypes;
import com.alibaba.alink.operator.common.feature.binning.BinningUtil;
import com.alibaba.alink.operator.common.feature.binning.FeatureBorder;
import com.alibaba.alink.operator.common.tree.Preprocessing;
import com.alibaba.alink.params.dataproc.HasHandleInvalid;
import com.alibaba.alink.params.feature.HasEncodeWithoutWoe;
import com.alibaba.alink.params.feature.HasEncodeWithoutWoeDefaultAsIndex;
import com.alibaba.alink.params.feature.QuantileDiscretizerPredictParams;
import com.alibaba.alink.params.shared.colname.HasOutputCol;
import com.alibaba.alink.params.shared.colname.HasOutputColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedCols;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.operator.common.tree.Preprocessing.isMissing;

/**
 * quantile discretizer model data mapper.
 */
public class QuantileDiscretizerModelMapper extends ModelMapper {
	private DiscretizerMapperBuilder mapperBuilder;

	public QuantileDiscretizerModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		mapperBuilder = new DiscretizerMapperBuilder(params, getDataSchema());
	}

	@Override
	public void loadModel(List<Row> modelRows) {
		QuantileDiscretizerModelDataConverter model = new QuantileDiscretizerModelDataConverter();
		model.load(modelRows);

		for (int i = 0; i < mapperBuilder.paramsBuilder.selectedCols.length; i++) {
			FeatureBorder border = model.data.get(mapperBuilder.paramsBuilder.selectedCols[i]);
			Preconditions.checkNotNull(border, "%s not found in model", mapperBuilder.paramsBuilder.selectedCols[i]);
			List<Bin.BaseBin> norm = border.bin.normBins;
			int size = norm.size();
			Long maxIndex = norm.get(0).getIndex();
			Long lastIndex = norm.get(size - 1).getIndex();
			for (int j = 0; j < norm.size(); ++j) {
				if (maxIndex < norm.get(j).getIndex()) {
					maxIndex = norm.get(j).getIndex();
				}
			}

			long maxIndexWithNull = Math.max(maxIndex, border.bin.nullBin.getIndex());

			switch (mapperBuilder.paramsBuilder.handleInvalidStrategy) {
				case KEEP:
					mapperBuilder.vectorSize.put(i, maxIndexWithNull + 1);
					break;
				case SKIP:
				case ERROR:
					mapperBuilder.vectorSize.put(i, maxIndex + 1);
					break;
				default:
					throw new UnsupportedOperationException("Unsupported now.");
			}

			if (mapperBuilder.paramsBuilder.dropLast) {
				mapperBuilder.dropIndex.put(i, lastIndex);
			}

			mapperBuilder.discretizers[i] = createQuantileDiscretizer(border, model.meta);
		}

		mapperBuilder.setAssembledVectorSize();
	}

	@Override
	public TableSchema getOutputSchema() {
		return mapperBuilder.paramsBuilder.outputColsHelper.getResultSchema();
	}

	@Override
	public Row map(Row row) throws Exception {
		return mapperBuilder.map(row);
	}

	public static class DiscretizerMapperBuilder implements Serializable {
		DiscretizerParamsBuilder paramsBuilder;
		int[] selectedColIndicesInData;
		Map<Integer, Long> vectorSize;
		Map<Integer, Long> dropIndex;
		Integer assembledVectorSize;
		NumericQuantileDiscretizer[] discretizers;
		Long[] predictIndices;

		public DiscretizerMapperBuilder(Params params, TableSchema dataSchema){
			paramsBuilder = new DiscretizerParamsBuilder(params, dataSchema, params.get(HasEncodeWithoutWoeDefaultAsIndex.ENCODE));
			this.selectedColIndicesInData = TableUtil.findColIndicesWithAssert(
				dataSchema,
				paramsBuilder.selectedCols
			);
			vectorSize = new HashMap<>();
			dropIndex = new HashMap<>();
			discretizers = new NumericQuantileDiscretizer[paramsBuilder.selectedCols.length];

		}

		void setAssembledVectorSize(){
			assembledVectorSize = vectorSize.values().stream().mapToInt(Long::intValue).sum();
			if (paramsBuilder.dropLast) {
				assembledVectorSize -= vectorSize.size();
			}

			predictIndices = new Long[paramsBuilder.selectedCols.length];
		}

		Row map(Row row){
			for (int i = 0; i < paramsBuilder.selectedCols.length; i++) {
				int colIdxInData = selectedColIndicesInData[i];
				Object val = row.getField(colIdxInData);
				int foundIndex = discretizers[i].findIndex(val);
				predictIndices[i] = (long) foundIndex;
				if (!discretizers[i].isValid(foundIndex)) {
					switch (paramsBuilder.handleInvalidStrategy) {
						case KEEP:
							break;
						case SKIP:
							predictIndices[i] = null;
							break;
						case ERROR:
							throw new RuntimeException("Unseen token: " + val);
						default:
							throw new IllegalArgumentException("Invalid handle invalid strategy.");
					}
				}
			}

			return paramsBuilder.outputColsHelper.getResultRow(
				row,
				setResultRow(
					predictIndices,
					paramsBuilder.encode,
					dropIndex,
					vectorSize,
					paramsBuilder.dropLast,
					assembledVectorSize)
			);
		}
	}

	public static class DiscretizerParamsBuilder implements Serializable {
		public HasEncodeWithoutWoe.Encode encode;
		public HasHandleInvalid.HandleInvalid handleInvalidStrategy;
		public String[] selectedCols;
		public OutputColsHelper outputColsHelper;
		public boolean dropLast;

        public DiscretizerParamsBuilder(Params params, TableSchema dataSchema, HasEncodeWithoutWoe.Encode encode) {
            String[] reservedCols = params.get(HasReservedCols.RESERVED_COLS);
            handleInvalidStrategy = params.get(HasHandleInvalid.HANDLE_INVALID);
			this.encode = encode;

            //To delete when open source
            if (!params.contains(HasOutputColsDefaultAsNull.OUTPUT_COLS) && params.contains(HasOutputCol.OUTPUT_COL)) {
                params.set(HasOutputColsDefaultAsNull.OUTPUT_COLS, new String[] {params.get(HasOutputCol.OUTPUT_COL)});
            }
            if (!params.contains(HasSelectedCols.SELECTED_COLS)) {
                Preconditions.checkArgument(
                    encode.equals(HasEncodeWithoutWoe.Encode.ASSEMBLED_VECTOR),
                    "Not given selectedCols, encode must be ASSEMBLED_VECTOR!"
                );
            } else {
                selectedCols = params.get(HasSelectedCols.SELECTED_COLS);
            }

            switch (encode) {
                case INDEX: {
                    String[] outputCols = params.get(HasOutputColsDefaultAsNull.OUTPUT_COLS);
                    if (null == outputCols) {
                        outputCols = selectedCols;
                    }
                    Preconditions.checkArgument(outputCols.length == selectedCols.length,
                        "Input column name is not match output column name.");
                    outputColsHelper = new OutputColsHelper(
                        dataSchema,
                        outputCols,
                        Arrays.stream(outputCols).map(x -> Types.LONG).toArray(TypeInformation[]::new),
                        reservedCols);
                    break;
                }
                case VECTOR: {
                    String[] outputCols = params.get(HasOutputColsDefaultAsNull.OUTPUT_COLS);
                    if (null == outputCols) {
                        outputCols = selectedCols;
                    }
                    Preconditions.checkArgument(outputCols.length == selectedCols.length,
                        "Input column name is not match output column name.");
                    outputColsHelper = new OutputColsHelper(
                        dataSchema,
                        outputCols,
                        Arrays.stream(outputCols).map(x -> VectorTypes.SPARSE_VECTOR).toArray(TypeInformation[]::new),
                        reservedCols);
                    break;
                }
                case ASSEMBLED_VECTOR: {
                    String[] outputCols = params.get(HasOutputColsDefaultAsNull.OUTPUT_COLS);
                    Preconditions.checkArgument(null != outputCols && outputCols.length == 1,
                        "When encode is ASSEMBLED_VECTOR, outputCols must be given and the length must be 1!");
                    outputColsHelper = new OutputColsHelper(
                        dataSchema,
                        outputCols[0],
                        VectorTypes.SPARSE_VECTOR,
                        reservedCols
                    );
                    break;
                }
                default: {
                    throw new IllegalArgumentException("Not support encode: " + encode.name());
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

			if (isMissing(dVal, zeroAsMissing)) {
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

			if (isMissing(lVal, zeroAsMissing)) {
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

	public static class GenericNumericQuantileDiscretizer implements NumericQuantileDiscretizer {
		FeatureBorder border;

		public GenericNumericQuantileDiscretizer(FeatureBorder border) {
			this.border = border;
		}

		@Override
		public boolean isValid(int index) {
			return index != border.bin.nullBin.getIndex();
		}

		@Override
		public int findIndex(Object number) {
			return BinningUtil.findNumericBin(number, border, border.colType).getIndex().intValue();
		}
	}

	public static NumericQuantileDiscretizer createQuantileDiscretizer(
		FeatureBorder featureBorder, Params params) {
		if (BinningUtil.isFeatureContinuous(featureBorder)) {
			List<Bin.BaseBin> norm = featureBorder.bin.normBins;
			int size = norm.size();
			boolean isLeftOpen = !norm.get(0).right().isOpen();
			int nullIndex = featureBorder.bin.nullBin.getIndex().intValue();

			int[] boundIndex = new int[size + 1];

			for (int i = 0; i < size; ++i) {
				boundIndex[i] = norm.get(i).getIndex().intValue();
			}

			boundIndex[size] = norm.get(size - 1).getIndex().intValue();

			if (featureBorder.colType.equals(BinTypes.ColType.LONG)) {
				long[] bounds = new long[size + 1];
				bounds[0] = -Long.MAX_VALUE;
				for (int i = 0; i < size - 1; ++i) {
					bounds[i + 1] = ((Number) norm.get(i).right().getRecord()).longValue();
				}
				bounds[size] = Long.MAX_VALUE;
				return new LongQuantileDiscretizer(
					bounds, isLeftOpen, boundIndex, nullIndex,
					params.get(Preprocessing.ZERO_AS_MISSING));
			} else {
				double[] bounds = new double[size + 1];
				bounds[0] = Double.NEGATIVE_INFINITY;
				for (int i = 0; i < size - 1; ++i) {
					bounds[i + 1] = ((Number) norm.get(i).right().getRecord()).doubleValue();
				}
				bounds[size] = Double.POSITIVE_INFINITY;
				return new DoubleNumericQuantileDiscretizer(
					bounds, isLeftOpen, boundIndex, nullIndex,
					params.get(Preprocessing.ZERO_AS_MISSING)
				);
			}
		} else {
			return new GenericNumericQuantileDiscretizer(featureBorder);
		}
	}

	/**
	 * Set the result row when predict index is gotten.
	 * @param predictIndices predict indices.
	 * @param encode Encode type.
	 * @param dropIndex If dropLast is true, this index is drop.
	 * @param vectorSize The vectorsizes of each column.
	 * @param dropLast Drop the last token or not.
	 * @param assembledVectorSize If encode is ASSEMBLED_VECTOR, this size is used.
	 * @return the result row.
	 */
	public static Row setResultRow(Long[] predictIndices,
								   HasEncodeWithoutWoe.Encode encode,
								   Map<Integer, Long> dropIndex,
								   Map<Integer, Long> vectorSize,
								   boolean dropLast,
								   int assembledVectorSize) {
		switch (encode) {
			case INDEX: {
				Row result = new Row(predictIndices.length);
				for (int i = 0; i < result.getArity(); i++) {
					result.setField(i, predictIndices[i]);
				}
				return result;
			}
			case VECTOR: {
				Row result = new Row(predictIndices.length);
				for (int i = 0; i < result.getArity(); i++) {
					if (predictIndices[i] == null) {
						result.setField(i, null);
						continue;
					}
					Tuple2<Integer, Integer> tuple = getVectorSizeAndIndex(predictIndices[i], dropIndex.get(i),
						vectorSize.get(i), dropLast);
					result.setField(i, null == tuple.f1 ? new SparseVector(tuple.f0)
						: new SparseVector(tuple.f0, new int[] {tuple.f1}, new double[] {1.0}));
				}
				return result;
			}
			case ASSEMBLED_VECTOR: {
				List<Integer> list = new ArrayList<>();
				int startIndex = 0;
				for (int i = 0; i < predictIndices.length; i++) {
					if (null == predictIndices[i]) {
						return Row.of(null);
					}
					Tuple2<Integer, Integer> tuple = getVectorSizeAndIndex(predictIndices[i], dropIndex.get(i),
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
				return Row.of(new SparseVector(assembledVectorSize, indices, values));
			}
			default: {
				throw new RuntimeException("Not support encode type!");
			}
		}
	}
	/**
	 * Return the vectorsize the final index.
	 * @param predictIndex predict index.
	 * @param dropIndex the index to drop, if dropLast is true, the return index is null.
	 * @param originVectorSize origin vectorsize.
	 * @param dropLast Drop the last index or not.
	 * @return (vectorSize, index)
	 */
	private static Tuple2<Integer, Integer> getVectorSizeAndIndex(Long predictIndex,
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
