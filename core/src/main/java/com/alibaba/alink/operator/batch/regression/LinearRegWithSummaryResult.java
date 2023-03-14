package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.LinearModelData;
import com.alibaba.alink.operator.common.linear.LinearModelDataConverter;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.operator.common.regression.LinearReg;
import com.alibaba.alink.operator.common.regression.LinearRegressionModel;
import com.alibaba.alink.operator.common.regression.LinearRegressionStepwise;
import com.alibaba.alink.operator.common.regression.LinearRegressionStepwiseModel;
import com.alibaba.alink.operator.common.regression.RidgeRegressionProcess;
import com.alibaba.alink.operator.batch.statistics.utils.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.statistics.SummaryResultTable;
import com.alibaba.alink.params.regression.LinearRegStepwiseTrainParams;
import com.alibaba.alink.params.regression.LinearRegStepwiseTrainParams.Method;
import com.alibaba.alink.params.regression.LinearRegTrainParams;
import com.alibaba.alink.params.regression.RidgeRegTrainParams;
import com.alibaba.alink.params.statistics.HasStatLevel_L1;

class LinearRegWithSummaryResult extends BatchOperator <LinearRegWithSummaryResult> {

	private static final long serialVersionUID = 9007546963532152447L;
	private static final ParamInfo <LinearRegType> REG_TYPE = ParamInfoFactory
		.createParamInfo("regType", LinearRegWithSummaryResult.LinearRegType.class)
		.setDescription("regType")
		.setRequired()
		.build();

	public LinearRegWithSummaryResult(Params params, LinearRegType regType) {
		super(params);
		this.getParams().set(REG_TYPE, regType);
	}

	private static LinearModelData getLinearModel(String modelName, String[] nameX, double[] beta) {
		LinearModelData retData = new LinearModelData();
		retData.coefVector = new DenseVector(beta.clone());
		retData.modelName = modelName;
		retData.linearModelType = LinearModelType.LinearReg;
		retData.featureNames = nameX;
		return retData;
	}

	@Override
	public LinearRegWithSummaryResult linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String yVar = getParams().get(LinearRegTrainParams.LABEL_COL);
		try {
			int yColIndex = TableUtil.findColIndex(in.getColNames(), yVar);
			if (yColIndex < 0) {
				throw new AkIllegalArgumentException("There is no column(" + yVar + ") in the training dataset.");
			}
			setOutput(StatisticsHelper.getSRT(in, HasStatLevel_L1.StatLevel.L2).flatMap(new MyReg(this.getParams(), in.getColTypes()[yColIndex])),
				new LinearModelDataConverter(in.getColTypes()[yColIndex]).getModelSchema());
			return this;
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new AkUnsupportedOperationException(ex.getMessage());
		}
	}

	public enum LinearRegType {
		common,
		ridge,
		stepwise
	}

	public static class MyReg implements FlatMapFunction <SummaryResultTable, Row> {
		private static final long serialVersionUID = 5647053026802533733L;
		private final Params params;
		private final TypeInformation labelType;

		public MyReg(Params params, TypeInformation labelType) {
			this.params = params;
			this.labelType = labelType;
		}

		@Override
		public void flatMap(SummaryResultTable srt, Collector <Row> collector) throws Exception {
			String yVar = this.params.get(LinearRegTrainParams.LABEL_COL);
			String[] xVars = this.params.get(LinearRegTrainParams.FEATURE_COLS);
			LinearRegType regType = this.params.get(REG_TYPE);
			LinearRegressionModel lrm;
			switch (regType) {
				case common:
					lrm = LinearReg.train(srt, yVar, xVars);
					new LinearModelDataConverter(labelType)
						.save(getLinearModel("Linear Regression", lrm.nameX, lrm.beta), collector);
					break;
				case ridge:
					double lambda = this.params.get(RidgeRegTrainParams.LAMBDA);
					RidgeRegressionProcess rrp = new RidgeRegressionProcess(srt, yVar, xVars);
					lrm = rrp.calc(new double[] {lambda}).lrModels[0];
					new LinearModelDataConverter(labelType)
						.save(getLinearModel("Ridge Regression", lrm.nameX, lrm.beta), collector);
					break;
				case stepwise:
					Method method = this.params.get(LinearRegStepwiseTrainParams.METHOD);
					LinearRegressionStepwiseModel lrsm = LinearRegressionStepwise.step(srt, yVar, xVars, method);
					lrm = lrsm.lrr;
					new LinearModelDataConverter(labelType)
						.save(getLinearModel("Linear Regression Stepwise", lrm.nameX, lrm.beta), collector);
					break;
				default:
					throw new AkUnsupportedOperationException("Not support this regression type : " + regType);
			}
		}
	}

}
