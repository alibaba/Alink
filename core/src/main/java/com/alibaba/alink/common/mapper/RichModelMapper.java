package com.alibaba.alink.common.mapper;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.params.mapper.RichModelMapperParams;

/**
 * Abstract class for mappers with model.
 *
 * <p>The RichModel is used to the classification, the regression or the clustering.
 * The output of the model mapper using RichModel as its model contains three part:
 * <ul>
 *     <li>The reserved columns from input</li>
 *     <li>The prediction result column</li>
 *     <li>The prediction detail column</li>
 * </ul>
 */
public abstract class RichModelMapper extends ModelMapper {

    /**
     * The output column helper which control the output format.
     *
     * @see OutputColsHelper
     */
    private final OutputColsHelper outputColsHelper;

    /**
     * The condition that the mapper output the prediction detail or not.
     */
    private final boolean isPredDetail;

    public RichModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
        super(modelSchema, dataSchema, params);
        String[] reservedColNames = this.params.get(RichModelMapperParams.RESERVED_COLS);
        String predResultColName = this.params.get(RichModelMapperParams.PREDICTION_COL);
        TypeInformation predResultColType = initPredResultColType();
        isPredDetail = params.contains(RichModelMapperParams.PREDICTION_DETAIL_COL);
        if (isPredDetail) {
            String predDetailColName = params.get(RichModelMapperParams.PREDICTION_DETAIL_COL);
            this.outputColsHelper = new OutputColsHelper(dataSchema,
                new String[]{predResultColName, predDetailColName},
                new TypeInformation[]{predResultColType, Types.STRING}, reservedColNames);
        } else {
            this.outputColsHelper = new OutputColsHelper(dataSchema, predResultColName, predResultColType,
                reservedColNames);
        }
    }

    /**
     * Initial the prediction result column type.
     *
     * <p>The subclass can override this method to initial the {@link OutputColsHelper}
     *
     * @return the type of the prediction result column
     */
    protected TypeInformation initPredResultColType() {
        return super.getModelSchema().getFieldTypes()[2];
    }

    @Override
    public TableSchema getOutputSchema() {
        return outputColsHelper.getResultSchema();
    }

    /**
     * Calculate the prediction result.
     *
     * @param row the input
     * @return the prediction result.
     */
    protected abstract Object predictResult(Row row) throws Exception;

    /**
     * Calculate the prediction result ant the prediction detail.
     *
     * @param row the input
     * @return The prediction result and the the prediction detail.
     */
    protected abstract Tuple2<Object, String> predictResultDetail(Row row) throws Exception;

    @Override
    public Row map(Row row) throws Exception {
        if (isPredDetail) {
            Tuple2<Object, String> t2 = predictResultDetail(row);
            return this.outputColsHelper.getResultRow(row, Row.of(t2.f0, t2.f1));
        } else {
            return this.outputColsHelper.getResultRow(row, Row.of(predictResult(row)));
        }
    }

}
