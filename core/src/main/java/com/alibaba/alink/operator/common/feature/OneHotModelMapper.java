package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.Functional;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelMapper.DiscreteParamsBuilder;
import com.alibaba.alink.params.dataproc.HasHandleInvalid;
import com.alibaba.alink.params.feature.HasEnableElse;
import com.alibaba.alink.params.feature.OneHotPredictParams;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This mapper maps some table columns to a binary vector. It encoding some columns to a key-value format.
 */
public class OneHotModelMapper extends ModelMapper {
	private static final long serialVersionUID = -6192598346177373139L;
	OneHotMapperBuilder mapperBuilder;

	/**
	 * Deal with the abnormal cases.
	 */
	enum InvalidStrategy {
		/**
		 * Enable else cases for those words unseen tokens. There are normal tokens, else tokens and null token. The
		 * index of normal tokens are the same with the index in the model. The index of null token is the size of
		 * normal tokens. The index of the unseen tokens is the size of normal tokens plus 1.
		 */
		ENABLE_ELSE_KEEP(maxIdx -> maxIdx + 3, (val, vectorSize) -> (val == null ? vectorSize - 2 : vectorSize - 1)),

		/**
		 * Enable else cases for those words unseen tokens. There are normal tokens, else tokens and null token. The
		 * index of normal tokens are the same with the index in the model. The index of the unseen tokens is the size
		 * of normal tokens. If the token is null, return null.
		 */
		ENABLE_ELSE_SKIP(maxIdx -> maxIdx + 2, (val, vectorSize) -> (val == null ? null : vectorSize - 1)),

		/**
		 * Enable else cases for those words unseen tokens. There are normal tokens and else tokens. The index of
		 * normal
		 * tokens are the same with the index in the model. The index of the unseen tokens is the size of normal
		 * tokens.
		 * If the token is null, throw exception.
		 */
		ENABLE_ELSE_ERROR(maxIdx -> maxIdx + 2, (val, vectorSize) -> {
			if (val == null) {
				throw new AkUnclassifiedErrorException("Input is null!");
			} else {
				return vectorSize - 1;
			}
		}),

		/**
		 * Disable else cases for those words unseen tokens. The unseen tokens are handled the same with the null
		 * token.
		 * The index of normal tokens are the same with the index in the model. The index of the unseen tokens and null
		 * token is the size of normal tokens.
		 */
		DISABLE_ELSE_KEEP(maxIdx -> maxIdx + 2, (val, vectorSize) -> vectorSize - 1),

		/**
		 * Disable else cases for those words unseen tokens. The unseen tokens are handled the same with the null
		 * token.
		 * The index of normal tokens are the same with the index in the model. If the token is unseen or null, return
		 * null.
		 */
		DISABLE_ELSE_SKIP(maxIdx -> maxIdx + 1, (val, vectorSize) -> null),

		/**
		 * Disable else cases for those words unseen tokens. The unseen tokens are handled the same with the null
		 * token.
		 * The index of normal tokens are the same with the index in the model. If the token is unseen or null, throw
		 * exception.
		 */
		DISABLE_ELSE_ERROR(maxIdx -> maxIdx + 1, (val, vectorSize) -> {
			throw new AkUnclassifiedErrorException("Unseen token: " + val);
		});

		final Functional.SerializableFunction <Long, Long> getVectorSizeFunc;
		final Functional.SerializableBiFunction <Object, Long, Long> predictIndexFunc;

		InvalidStrategy(Functional.SerializableFunction <Long, Long> getVectorSizeFunc,
						Functional.SerializableBiFunction <Object, Long, Long> predictIndexFunc) {
			this.getVectorSizeFunc = getVectorSizeFunc;
			this.predictIndexFunc = predictIndexFunc;
		}

		/**
		 * Get the method to handle unseen token and null token.
		 *
		 * @param enableElse            If filter of low frequency is set in training, enableElse is true.
		 * @param handleInvalidStrategy strategy to handle invalid cases(unseen token and null token)
		 * @return the method to handle unseen token and null token.
		 */
		public static InvalidStrategy valueOf(boolean enableElse,
											  HasHandleInvalid.HandleInvalid handleInvalidStrategy) {
			switch (handleInvalidStrategy) {
				case KEEP: {
					if (enableElse) {
						return ENABLE_ELSE_KEEP;
					} else {
						return DISABLE_ELSE_KEEP;
					}
				}
				case SKIP: {
					if (enableElse) {
						return ENABLE_ELSE_SKIP;
					} else {
						return DISABLE_ELSE_SKIP;
					}
				}
				case ERROR:
					if (enableElse) {
						return ENABLE_ELSE_ERROR;
					} else {
						return DISABLE_ELSE_ERROR;
					}
				default: {
					throw new AkUnsupportedOperationException("Invalid handle invalid strategy.");
				}
			}
		}
	}

	/**
	 * Constructor.
	 *
	 * @param modelSchema the model schema.
	 * @param dataSchema  the data schema.
	 * @param params      the parameters of mapper.
	 */
	public OneHotModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	/**
	 * get size of vector with all columns encoding.
	 * @return size of vector.
	 */
	public int getSize() {
		return mapperBuilder.assembledVectorSize;
	}

	/**
	 * Load row type model and save it in OneHotModelData.
	 *
	 * @param modelRows row type model.
	 */
	@Override
	public void loadModel(List <Row> modelRows) {
		OneHotModelData model = new OneHotModelDataConverter().load(modelRows);
		String[] trainColNames = model.modelData.meta.get(HasSelectedCols.SELECTED_COLS);

		//to be compatible with previous versions
		if (null == mapperBuilder.getSelectedCols()) {
			mapperBuilder.setSelectedCols(trainColNames);
		}

		mapperBuilder.setSelectedColIndicesInData(super.getDataSchema());
		mapperBuilder.setInvalidStrategy(model.modelData.meta.get(HasEnableElse.ENABLE_ELSE));
		int[] selectedColIndicesInModel = TableUtil.findColIndices(trainColNames,
			mapperBuilder.getSelectedCols());
		for (int i : selectedColIndicesInModel) {
			if (i == -1) {
				mapperBuilder.setSelectedCols(trainColNames);
				selectedColIndicesInModel = TableUtil.findColIndices(trainColNames,
					mapperBuilder.getSelectedCols());
				break;
			}
		}

		for (int i = 0; i < mapperBuilder.getSelectedCols().length; i++) {
			Map <String, Long> mapper = new HashMap <>();
			int colIdxInModel = selectedColIndicesInModel[i];

			AkPreconditions.checkArgument(colIdxInModel >= 0, "Can not find %s in model!",
				mapperBuilder.getSelectedCols()[i]);
			for (Tuple3 <Integer, String, Long> record : model.modelData.tokenAndIndex) {
				if (record.f0 == colIdxInModel) {
					String token = record.f1;
					Long index = record.f2;
					mapper.put(token, index);
				}
			}
			mapperBuilder.updateIndexMapper(i, mapper);
			Long maxIdx = mapper.values().stream().distinct().count() - 1;
			mapper.values().forEach(index -> AkPreconditions
				.checkArgument(index >= 0 && index <= maxIdx, "Index must be continuous!"));
			mapperBuilder.updateMaxIdx(i, maxIdx);
		}
		mapperBuilder.setAssembledVectorSize();
	}

	static class OneHotMapperBuilder implements Serializable {
		private static final long serialVersionUID = 1940186540656289256L;
		DiscreteParamsBuilder paramsBuilder;
		int[] selectedColIndicesInData;
		Map <Integer, Long> vectorSize;
		Map <Integer, Long> dropIndex;
		Map <Integer, Map <String, Long>> indexMapper;
		Integer assembledVectorSize;
		OneHotModelMapper.InvalidStrategy invalidStrategy;

		public OneHotMapperBuilder(Params params, TableSchema dataSchema) {
			paramsBuilder = new DiscreteParamsBuilder(params, dataSchema,
				params.get(OneHotPredictParams.ENCODE));
			indexMapper = new HashMap <>();
			vectorSize = new HashMap <>();
			dropIndex = new HashMap <>();
		}

		String[] getSelectedCols() {
			return paramsBuilder.selectedCols;
		}

		void setSelectedCols(String[] selectedCols) {
			paramsBuilder.selectedCols = selectedCols;
		}

		void setInvalidStrategy(boolean enableElse) {
			this.invalidStrategy = OneHotModelMapper.InvalidStrategy.valueOf(enableElse,
				paramsBuilder.handleInvalidStrategy);
		}

		void setSelectedColIndicesInData(TableSchema tableSchema) {
			selectedColIndicesInData = new int[paramsBuilder.selectedCols.length];
			for (int i = 0; i < paramsBuilder.selectedCols.length; i++) {
				selectedColIndicesInData[i] = TableUtil.findColIndex(tableSchema, paramsBuilder.selectedCols[i]);
				AkPreconditions.checkArgument(selectedColIndicesInData[i] >= 0, "Can not find %s in data",
					paramsBuilder.selectedCols[i]);
			}
		}

		void updateIndexMapper(Integer key, Map <String, Long> value) {
			indexMapper.put(key, value);
		}

		void updateMaxIdx(Integer key, Long value) {
			vectorSize.put(key, value);
		}

		void setAssembledVectorSize() {
			for (Map.Entry <Integer, Long> entry : vectorSize.entrySet()) {
				int index = entry.getKey();
				long maxIdx = entry.getValue();
				vectorSize.put(index, invalidStrategy.getVectorSizeFunc.apply(maxIdx));
				if (paramsBuilder.dropLast) {
					dropIndex.put(index, maxIdx);
					//If threshold is too big, there is no seen token in the model
					//the vectorSize mapper save the vectorSize without droplast, in the predict period, if dropLast is
					//true, the vectorSize will minus 1, so if maxIndx < 0, must plus 1 at the beginning
					if (maxIdx < 0) {
						vectorSize.put(index, vectorSize.get(index) + 1);
					}
				}
			}

			assembledVectorSize = vectorSize.values().stream().mapToInt(Long::intValue).sum();
			if (paramsBuilder.dropLast) {
				assembledVectorSize -= vectorSize.size();
			}
		}

		void map(SlicedSelectedSample selection, SlicedResult result) {
			Long[] predictIndices = new Long[paramsBuilder.selectedCols.length];
			for (int i = 0; i < paramsBuilder.selectedCols.length; i++) {
				Map <String, Long> mapper = indexMapper.get(i);
				Object val = selection.get(i);
				predictIndices[i] = null == val ? null : mapper.get(String.valueOf(val));

				if (predictIndices[i] == null) {
					predictIndices[i] = invalidStrategy.predictIndexFunc.apply(val, vectorSize.get(i));
				}
			}

			Row res = QuantileDiscretizerModelMapper
				.setResultRow(
					predictIndices,
					paramsBuilder.encode,
					dropIndex,
					vectorSize,
					paramsBuilder.dropLast,
					assembledVectorSize);
			for (int i = 0; i < result.length(); i++) {
				result.set(i, res.getField(i));
			}
		}
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema modelSchema,
																						   TableSchema dataSchema,
																						   Params params) {
		mapperBuilder = new OneHotMapperBuilder(params, dataSchema);
		if (mapperBuilder.getSelectedCols() == null) {
			mapperBuilder.setSelectedCols(dataSchema.getFieldNames());
		}
		return Tuple4.of(mapperBuilder.paramsBuilder.selectedCols,
			mapperBuilder.paramsBuilder.resultCols,
			mapperBuilder.paramsBuilder.resultColTypes,
			mapperBuilder.paramsBuilder.reservedCols);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		mapperBuilder.map(selection, result);
	}

}
