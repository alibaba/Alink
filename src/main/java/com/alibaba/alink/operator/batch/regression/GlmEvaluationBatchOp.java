package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.regression.glm.FamilyLink;
import com.alibaba.alink.operator.common.regression.glm.GlmUtil;
import com.alibaba.alink.params.regression.GlmTrainParams;

public class GlmEvaluationBatchOp extends BatchOperator<GlmEvaluationBatchOp>
    implements GlmTrainParams<GlmEvaluationBatchOp> {

    public GlmEvaluationBatchOp() {

    }

    public GlmEvaluationBatchOp(Params params) {
        super(params);
    }

    @Override
    public GlmEvaluationBatchOp linkFrom(BatchOperator<?>... inputs) {
        checkOpSize(2, inputs);

        BatchOperator<?> model = inputs[0];
        BatchOperator<?> in = inputs[1];

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

        FamilyLink familyLink = new FamilyLink(familyName, variancePower, linkName, linkPower);

        int numFeature = featureColNames.length;

        DataSet<Row> data = GlmUtil.preProc(in, featureColNames, offsetColName, weightColName, labelColName);

        DataSet<GlmUtil.WeightedLeastSquaresModel> wlsModel = model.getDataSet().mapPartition(
            new GlmUtil.GlmModelToWlsModel());
        DataSet<Row> residual = GlmUtil.residual(wlsModel, data, numFeature, familyLink);
        DataSet<Row> aggSummay = GlmUtil.aggSummary(residual, wlsModel,
            numFeature, familyLink, regParam, numIter, epsilon, fitIntercept);

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

        this.setSideOutputTables(new Table[]{
            DataSetConversionUtil.toTable(getMLEnvironmentId(),
                residual, residualColNames, residualColTypes)
        });

        //summary
        String[] summaryColNames = new String[1];
        TypeInformation[] summaryColTypes = new TypeInformation[1];
        summaryColNames[0] = "summary";
        summaryColTypes[0] = Types.STRING;

        this.setOutput(aggSummay, summaryColNames, summaryColTypes);

        return this;
    }
}
