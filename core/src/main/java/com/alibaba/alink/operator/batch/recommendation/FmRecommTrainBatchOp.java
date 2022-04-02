package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.FmClassifierTrainBatchOp;
import com.alibaba.alink.operator.batch.regression.FmRegressorTrainBatchOp;
import com.alibaba.alink.operator.batch.sql.LeftOuterJoinBatchOp;
import com.alibaba.alink.operator.common.utils.PackBatchOperatorUtil;
import com.alibaba.alink.params.recommendation.FmRecommTrainParams;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;
import com.alibaba.alink.pipeline.feature.OneHotEncoder;
import org.apache.commons.lang.ArrayUtils;

import java.util.Arrays;

/**
 * Fm train batch op for recommendation.
 */

@InputPorts(values = {
    @PortSpec(PortType.DATA),
    @PortSpec(value = PortType.DATA, isOptional = true),
    @PortSpec(value = PortType.DATA, isOptional = true)
})
@OutputPorts(values = {
    @PortSpec(PortType.MODEL),
    @PortSpec(value = PortType.DATA, desc = PortDesc.USER_FACTOR),
    @PortSpec(value = PortType.DATA, desc = PortDesc.ITEM_FACTOR),
    @PortSpec(value = PortType.DATA, desc = PortDesc.APPEND_USER_FACTOR, isOptional = true),
    @PortSpec(value = PortType.DATA, desc = PortDesc.APPEND_ITEM_FACTOR, isOptional = true)
})
@ParamSelectColumnSpec(name = "userCol")
@ParamSelectColumnSpec(name = "itemCol")
@ParamSelectColumnSpec(name = "rateCol",
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES)

@ParamSelectColumnSpec(name = "userFeatureCols", portIndices = 1,
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@ParamSelectColumnSpec(name = "userCategoricalFeatureCols", portIndices = 1)
@ParamSelectColumnSpec(name = "itemFeatureCols", portIndices = 2,
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@ParamSelectColumnSpec(name = "itemCategoricalFeatureCols", portIndices = 2)

@NameCn("FM推荐训练")
public final class FmRecommTrainBatchOp
        extends BatchOperator<FmRecommTrainBatchOp>
        implements FmRecommTrainParams<FmRecommTrainBatchOp> {

    private static final long serialVersionUID = 4783783487314711500L;
    boolean implicitFeedback = false;

    public FmRecommTrainBatchOp() {
        this(new Params());
    }

    public FmRecommTrainBatchOp(Params params) {
        super(params);
    }

    private static String[] subtract(String[] a, String[] b) {
        String[] ret = new String[a.length];
        int cnt = 0;
        for (String s : a) {
            if (TableUtil.findColIndex(b, s) < 0) {
                ret[cnt++] = s;
            }
        }
        return Arrays.copyOf(ret, cnt);
    }

    public static class ConvertVec extends ScalarFunction {
        private static final long serialVersionUID = -8905679791356243034L;

        public String eval(Vector v) {
            return VectorUtil.serialize(v);
        }
    }

    public static class CheckNotNull extends ScalarFunction {
        private static final long serialVersionUID = -8064346886560345966L;

        public String eval(String v) {
            if (v == null) {
                throw new RuntimeException("feature vector is null, perhaps some user/item feature is missing.");
            }
            return v;
        }
    }

    private static BatchOperator <?> createFeatureVectors(BatchOperator <?> featureTable, String idCol,
                                                      String[] featureCols, String[] categoricalCols) {
        TableUtil.assertSelectedColExist(featureCols, categoricalCols);
        String[] numericalCols = subtract(featureCols, categoricalCols);
        final Long envId = featureTable.getMLEnvironmentId();

        if (categoricalCols.length > 0) {
            OneHotEncoder onehot = new OneHotEncoder().setMLEnvironmentId(envId)
                    .setSelectedCols(categoricalCols)
                    .setOutputCols("__fm_features__").setDropLast(false);
            featureTable = onehot.fit(featureTable).transform(featureTable);
            numericalCols = (String[]) ArrayUtils.add(numericalCols, "__fm_features__");
        }
        VectorAssembler va = new VectorAssembler().setMLEnvironmentId(envId)
                .setSelectedCols(numericalCols)
                .setOutputCol("__fm_features__").setReservedCols(idCol);
        featureTable = va.transform(featureTable);
        featureTable = featureTable.udf("__fm_features__", "__fm_features__", new ConvertVec());
        return featureTable;
    }

    /**
     * There are 3 input tables: 1) user-item-label table, 2) user features table, 3) item features table.
     * If user or item features table is missing, then use their IDs as features.
     */
    @Override
    public FmRecommTrainBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> samplesOp = inputs[0];
        final Long envId = samplesOp.getMLEnvironmentId();
        BatchOperator<?> userFeaturesOp = inputs.length >= 2 ? inputs[1] : null;
        BatchOperator<?> itemFeaturesOp = inputs.length >= 3 ? inputs[2] : null;
        Params params = getParams();

        String userCol = params.get(USER_COL);
        String itemCol = params.get(ITEM_COL);
        String labelCol = params.get(RATE_COL);
        String[] userFeatureCols = params.get(USER_FEATURE_COLS);
        String[] itemFeatureCols = params.get(ITEM_FEATURE_COLS);
        String[] userCateFeatureCols = params.get(USER_CATEGORICAL_FEATURE_COLS);
        String[] itemCateFeatureCols = params.get(ITEM_CATEGORICAL_FEATURE_COLS);

        if (userFeaturesOp == null) {
            userFeaturesOp = samplesOp.select("`" + userCol + "`").distinct();
            userFeatureCols = new String[]{userCol};
            userCateFeatureCols = new String[]{userCol};
        } else {
            Preconditions.checkArgument(
                    TableUtil.findColTypeWithAssert(userFeaturesOp.getSchema(), userCol).equals(
                            TableUtil.findColTypeWithAssert(samplesOp.getSchema(), userCol))
                    , "user column type mismatch");
        }

        if (itemFeaturesOp == null) {
            itemFeaturesOp = samplesOp.select("`" + itemCol + "`").distinct();
            itemFeatureCols = new String[]{itemCol};
            itemCateFeatureCols = new String[]{itemCol};
        } else {
            Preconditions.checkArgument(
                    TableUtil.findColTypeWithAssert(itemFeaturesOp.getSchema(), itemCol).equals(
                            TableUtil.findColTypeWithAssert(samplesOp.getSchema(), itemCol))
                    , "item column type mismatch");
        }

        BatchOperator <?> history = samplesOp.select(new String[]{userCol, itemCol});
        userFeaturesOp = createFeatureVectors(userFeaturesOp, userCol, userFeatureCols, userCateFeatureCols);
        itemFeaturesOp = createFeatureVectors(itemFeaturesOp, itemCol, itemFeatureCols, itemCateFeatureCols);

        LeftOuterJoinBatchOp joinOp1 = new LeftOuterJoinBatchOp()
                .setMLEnvironmentId(envId)
                .setJoinPredicate("a.`" + userCol + "`=" + "b.`" + userCol + "`")
                .setSelectClause("a.*, b.__fm_features__ as __user_features__");
        LeftOuterJoinBatchOp joinOp2 = new LeftOuterJoinBatchOp()
                .setMLEnvironmentId(envId)
                .setJoinPredicate("a.`" + itemCol + "`=" + "b.`" + itemCol + "`")
                .setSelectClause("a.*, b.__fm_features__ as __item_features__");
        samplesOp = joinOp1.linkFrom(samplesOp, userFeaturesOp);
        samplesOp = joinOp2.linkFrom(samplesOp, itemFeaturesOp);
        samplesOp = samplesOp.udf("__user_features__", "__user_features__", new CheckNotNull());
        samplesOp = samplesOp.udf("__item_features__", "__item_features__", new CheckNotNull());

        VectorAssembler va = new VectorAssembler().setMLEnvironmentId(envId)
                .setSelectedCols("__user_features__", "__item_features__").setOutputCol("__alink_features__").setReservedCols(
                        labelCol);
        samplesOp = va.transform(samplesOp);
        BatchOperator <?> fmModel;

        if (!implicitFeedback) {
            fmModel = new FmRegressorTrainBatchOp(params)
                    .setLabelCol(params.get(RATE_COL)).setVectorCol("__alink_features__").setMLEnvironmentId(envId);
        } else {
            fmModel = new FmClassifierTrainBatchOp(params)
                    .setLabelCol(params.get(RATE_COL)).setVectorCol("__alink_features__").setMLEnvironmentId(envId);
        }
        fmModel.linkFrom(samplesOp);

        BatchOperator <?> model = PackBatchOperatorUtil.packBatchOps(
                new BatchOperator<?>[]{fmModel, userFeaturesOp, itemFeaturesOp, history});
        setOutputTable(model.getOutputTable());
        return this;
    }
}
