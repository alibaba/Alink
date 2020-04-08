package com.alibaba.alink.operator.common.feature;

import com.alibaba.alink.params.feature.OneHotPredictParams;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.Functional;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.dataproc.StringIndexerUtil;
import com.alibaba.alink.params.feature.HasEnableElse;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This mapper maps some table columns to a binary vector. It encoding some columns to a key-value format.
 */
public class OneHotModelMapper extends ModelMapper {
    private OneHotMapperBuilder mapperBuilder;

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
         * Enable else cases for those words unseen tokens. There are normal tokens and else tokens. The index of normal
         * tokens are the same with the index in the model. The index of the unseen tokens is the size of normal tokens.
         * If the token is null, throw exception.
         */
        ENABLE_ELSE_ERROR(maxIdx -> maxIdx + 2, (val, vectorSize) -> {
            if (val == null) {
                throw new RuntimeException("Input is null!");
            } else {
                return vectorSize - 1;
            }
        }),

        /**
         * Disable else cases for those words unseen tokens. The unseen tokens are handled the same with the null token.
         * The index of normal tokens are the same with the index in the model. The index of the unseen tokens and null
         * token is the size of normal tokens.
         */
        DISABLE_ELSE_KEEP(maxIdx -> maxIdx + 2, (val, vectorSize) -> vectorSize - 1),

        /**
         * Disable else cases for those words unseen tokens. The unseen tokens are handled the same with the null token.
         * The index of normal tokens are the same with the index in the model. If the token is unseen or null, return
         * null.
         */
        DISABLE_ELSE_SKIP(maxIdx -> maxIdx + 1, (val, vectorSize) -> null),

        /**
         * Disable else cases for those words unseen tokens. The unseen tokens are handled the same with the null token.
         * The index of normal tokens are the same with the index in the model. If the token is unseen or null, throw
         * exception.
         */
        DISABLE_ELSE_ERROR(maxIdx -> maxIdx + 1, (val, vectorSize) -> {
            throw new RuntimeException("Unseen token: " + val);
        });

        final Functional.SerializableFunction<Long, Long> getVectorSizeFunc;
        final Functional.SerializableBiFunction<Object, Long, Long> predictIndexFunc;

        InvalidStrategy(Functional.SerializableFunction<Long, Long> getVectorSizeFunc,
                        Functional.SerializableBiFunction<Object, Long, Long> predictIndexFunc) {
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
                                              StringIndexerUtil.HandleInvalidStrategy handleInvalidStrategy) {
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
                    throw new IllegalArgumentException("Invalid handle invalid strategy.");
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
        mapperBuilder = new OneHotMapperBuilder(params, dataSchema);
    }

    /**
     * Load row type model and save it in OneHotModelData.
     *
     * @param modelRows row type model.
     */
    @Override
    public void loadModel(List<Row> modelRows) {
        OneHotModelData model = new OneHotModelDataConverter().load(modelRows);
        String[] trainColNames = model.modelData.meta.get(HasSelectedCols.SELECTED_COLS);
        if (null == mapperBuilder.getSelectedCols()) {
            mapperBuilder.setSelectedCols(trainColNames);
        }

        mapperBuilder.setSelectedColIndicesInData(super.getDataSchema());
        mapperBuilder.setInvalidStrategy(model.modelData.meta.get(HasEnableElse.ENABLE_ELSE));
        int[] selectedColIndicesInModel = TableUtil.findColIndicesWithAssert(trainColNames, mapperBuilder.getSelectedCols());

        for (int i = 0; i < mapperBuilder.getSelectedCols().length; i++) {
            Map<String, Long> mapper = new HashMap<>();
            int colIdxInModel = selectedColIndicesInModel[i];

            Preconditions.checkArgument(colIdxInModel >= 0, "Can not find %s in model!",
                mapperBuilder.getSelectedCols()[i]);
            for (Tuple3<Integer, String, Long> record : model.modelData.tokenAndIndex) {
                if (record.f0 == colIdxInModel) {
                    String token = record.f1;
                    Long index = record.f2;
                    mapper.put(token, index);
                }
            }
            mapperBuilder.updateIndexMapper(i, mapper);
            Long maxIdx = mapper.values().stream().distinct().count() - 1;
            mapper.values().forEach(index -> Preconditions
                .checkArgument(index >= 0 && index <= maxIdx, "Index must be continuous!"));
            mapperBuilder.updateVectorSize(i, maxIdx);
        }
        mapperBuilder.setAssembledVectorSize();
    }

    static class OneHotMapperBuilder implements Serializable {
        QuantileDiscretizerModelMapper.DiscretizerParamsBuilder paramsBuilder;
        int[] selectedColIndicesInData;
        Map<Integer, Long> vectorSize;
        Map<Integer, Long> dropIndex;
        Map<Integer, Map<String, Long>> indexMapper;
        Integer assembledVectorSize;
        OneHotModelMapper.InvalidStrategy invalidStrategy;

        public OneHotMapperBuilder(Params params, TableSchema dataSchema) {
            paramsBuilder = new QuantileDiscretizerModelMapper.DiscretizerParamsBuilder(params, dataSchema,
                params.get(OneHotPredictParams.ENCODE));
            indexMapper = new HashMap<>();
            vectorSize = new HashMap<>();
            dropIndex = new HashMap<>();
        }

        String[] getSelectedCols() {
            return paramsBuilder.selectedCols;
        }

        void setSelectedCols(String[] selectedCols) {
            paramsBuilder.selectedCols = selectedCols;
        }

        void setInvalidStrategy(boolean enableElse){
            this.invalidStrategy = OneHotModelMapper.InvalidStrategy.valueOf(enableElse, paramsBuilder.handleInvalidStrategy);
        }

        void setSelectedColIndicesInData(TableSchema tableSchema){
            selectedColIndicesInData = new int[paramsBuilder.selectedCols.length];
            for(int i = 0; i < paramsBuilder.selectedCols.length; i++) {
                selectedColIndicesInData[i] = TableUtil.findColIndex(tableSchema, paramsBuilder.selectedCols[i]);
                Preconditions.checkArgument(selectedColIndicesInData[i] >= 0, "Can not find %s in data",
                    paramsBuilder.selectedCols[i]);
            }
        }

        void updateIndexMapper(Integer key, Map<String, Long> value){
            indexMapper.put(key, value);
        }

        void updateVectorSize(Integer key, Long value){
            vectorSize.put(key, value);
        }

        void setAssembledVectorSize() {
            for (Map.Entry<Integer, Long> entry : vectorSize.entrySet()) {
                int index = entry.getKey();
                long maxIdx = entry.getValue();
                vectorSize.put(index, invalidStrategy.getVectorSizeFunc.apply(maxIdx));
                if (paramsBuilder.dropLast) {
                    dropIndex.put(index, maxIdx);
                }
            }

            assembledVectorSize = vectorSize.values().stream().mapToInt(Long::intValue).sum();
            if (paramsBuilder.dropLast) {
                assembledVectorSize -= vectorSize.size();
            }
        }

        Row map(Row row) {
            Long[] predictIndices = new Long[paramsBuilder.selectedCols.length];
            for (int i = 0; i < paramsBuilder.selectedCols.length; i++) {
                Map<String, Long> mapper = indexMapper.get(i);
                int colIdxInData = selectedColIndicesInData[i];
                Object val = row.getField(colIdxInData);
                predictIndices[i] = null == val ? null : mapper.get(String.valueOf(val));

                if (predictIndices[i] == null) {
                    predictIndices[i] = invalidStrategy.predictIndexFunc.apply(val, vectorSize.get(i));
                }
            }
            return paramsBuilder
                .outputColsHelper
                .getResultRow(
                    row,
                    QuantileDiscretizerModelMapper
                        .setResultRow(
                            predictIndices,
                            paramsBuilder.encode,
                            dropIndex,
                            vectorSize,
                            paramsBuilder.dropLast,
                            assembledVectorSize)
                );
        }
    }

    /**
     * Get output schema.
     *
     * @return output schema.
     */
    @Override
    public TableSchema getOutputSchema() {
        return mapperBuilder.paramsBuilder.outputColsHelper.getResultSchema();
    }

    @Override
    public Row map(Row row) throws Exception {
        return mapperBuilder.map(row);
    }
}
