package com.alibaba.alink.operator.batch.regression;

import com.alibaba.alink.operator.common.regression.GlmModelData;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.regression.GlmModelDataConverter;
import com.alibaba.alink.operator.common.regression.glm.FamilyLink;
import com.alibaba.alink.operator.common.regression.glm.GlmUtil;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.params.regression.GlmTrainParams;

/**
 * Generalized Linear Model.
 */
public final class GlmTrainBatchOp extends BatchOperator<GlmTrainBatchOp>
    implements GlmTrainParams<GlmTrainBatchOp> {

    /**
     * default constructor.
     */
    public GlmTrainBatchOp() {

    }

    /**
     * constructor.
     * @param params
     */
    public GlmTrainBatchOp(Params params) {
        super(params);
    }

    @Override
    public GlmTrainBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);

        String[] featureColNames = getFeatureCols();
        String labelColName = getLabelCol();
        String weightColName = getWeightCol();
        String offsetColName = getOffsetCol();

        String familyName = getFamily();
        String linkName = getLink();
        double variancePower = getVariancePower();
        double linkPower = getLinkPower();

        int numIter = getMaxIter();
        double epsilon = getEpsilon();

        boolean fitIntercept = getFitIntercept();
        double regParam = getRegParam();

        int numFeature = featureColNames.length;

        FamilyLink familyLink = new FamilyLink(familyName, variancePower, linkName, linkPower);

        DataSet<Row> data = GlmUtil.preProc(in, featureColNames, offsetColName, weightColName, labelColName);

        DataSet<GlmUtil.WeightedLeastSquaresModel> finalModel =
            GlmUtil.train(data, numFeature, familyLink, regParam, fitIntercept, numIter, epsilon);

        this.setOutput(finalModel.mapPartition(
            new BuildModel(featureColNames, offsetColName, weightColName, labelColName,
                familyName, variancePower, linkName, linkPower, fitIntercept, numIter, epsilon)).setParallelism(1),
            new GlmModelDataConverter().getModelSchema());

        //residual
        String[] residualColNames = new String[numFeature + 4 + 4];
        TypeInformation[] residualColTypes = new TypeInformation[numFeature + 4 + 4];
        for (int i = 0; i < numFeature; i++) {
            residualColNames[i] = featureColNames[i];
            residualColTypes[i] = Types.DOUBLE;
        }
        residualColNames[numFeature] = "label";
        residualColTypes[numFeature] = Types.DOUBLE;
        residualColNames[numFeature + 1] = "weight";
        residualColTypes[numFeature + 1] = Types.DOUBLE;
        residualColNames[numFeature + 2] = "offset";
        residualColTypes[numFeature + 2] = Types.DOUBLE;
        residualColNames[numFeature + 3] = "pred";
        residualColTypes[numFeature + 3] = Types.DOUBLE;
        residualColNames[numFeature + 4] = "residualdevianceResiduals";
        residualColTypes[numFeature + 4] = Types.DOUBLE;
        residualColNames[numFeature + 5] = "pearsonResiduals";
        residualColTypes[numFeature + 5] = Types.DOUBLE;
        residualColNames[numFeature + 6] = "workingResiduals";
        residualColTypes[numFeature + 6] = Types.DOUBLE;
        residualColNames[numFeature + 7] = "responseResiduals";
        residualColTypes[numFeature + 7] = Types.DOUBLE;

        DataSet<Row> residual = GlmUtil.residual(finalModel, data, numFeature, familyLink);

        //summary
        String[] summaryColNames = new String[1];
        TypeInformation[] summaryColTypes = new TypeInformation[1];
        summaryColNames[0] = "summary";
        summaryColTypes[0] = Types.STRING;

        this.setSideOutputTables(new Table[]{
            DataSetConversionUtil.toTable(getMLEnvironmentId(),
                residual, residualColNames, residualColTypes),
            DataSetConversionUtil.toTable(getMLEnvironmentId(), GlmUtil.aggSummary(residual, finalModel,
                numFeature, familyLink, regParam, numIter, epsilon, fitIntercept),
                summaryColNames, summaryColTypes)
        });

        return this;
    }

    /**
     * build glm model.
     */
    private static class BuildModel implements MapPartitionFunction<GlmUtil.WeightedLeastSquaresModel, Row> {
        private String[] featureColNames;
        private String offsetColName;
        private String weightColName;
        private String labelColName;

        private String familyName;
        private double variancePower;
        private String linkName;
        private double linkPower;

        private boolean fitIntercept;
        private int numIter;
        private double epsilon;

        public BuildModel(String[] featureColNames, String offsetColName,
                          String weightColName, String labelColName,
                          String familyName, double variancePower,
                          String linkName, double linkPower,
                          Boolean fitIntercept, int numIter, double epsilon) {
            this.featureColNames = featureColNames;
            this.offsetColName = offsetColName;
            this.weightColName = weightColName;
            this.labelColName = labelColName;

            this.familyName = familyName;
            this.variancePower = variancePower;
            this.linkName = linkName;
            this.linkPower = linkPower;

            this.fitIntercept = fitIntercept;
            this.numIter = numIter;
            this.epsilon = epsilon;
        }

        @Override
        public void mapPartition(Iterable<GlmUtil.WeightedLeastSquaresModel> iterable, Collector<Row> result)
            throws Exception {
            GlmUtil.WeightedLeastSquaresModel model = iterable.iterator().next();
            GlmModelDataConverter outModel = new GlmModelDataConverter();
            GlmModelData modelData = new GlmModelData();
            modelData.featureColNames = featureColNames;
            modelData.offsetColName = offsetColName;
            modelData.weightColName = weightColName;
            modelData.labelColName = labelColName;

            modelData.familyName = familyName;
            modelData.variancePower = variancePower;
            modelData.linkName = linkName;
            modelData.linkPower = linkPower;

            modelData.coefficients = model.coefficients;
            modelData.intercept = model.intercept;
            modelData.diagInvAtWA = model.diagInvAtWA;

            modelData.fitIntercept = fitIntercept;
            modelData.numIter = numIter;
            modelData.epsilon = epsilon;

            outModel.save(modelData, result);
        }

    }

}
