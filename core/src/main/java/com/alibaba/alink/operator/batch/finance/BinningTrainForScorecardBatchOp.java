package com.alibaba.alink.operator.batch.finance;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.viz.AlinkViz;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.BinningTrainBatchOp;
import com.alibaba.alink.params.feature.HasConstraint;
import com.alibaba.alink.params.finance.BinningTrainForScorecardParams;
import com.alibaba.alink.params.shared.HasMLEnvironmentId;

@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {
	@PortSpec(value = PortType.MODEL, desc = PortDesc.OUTPUT_RESULT),
	@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)
})
@ParamSelectColumnSpec(name = "selectedCols", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("评分卡分箱训练")
@NameEn("Score Train")
public final class BinningTrainForScorecardBatchOp extends BatchOperator <BinningTrainForScorecardBatchOp>
	implements BinningTrainForScorecardParams <BinningTrainForScorecardBatchOp>,
	AlinkViz <BinningTrainForScorecardBatchOp> {

	private static final long serialVersionUID = -1215494859549421782L;
	public static TableSchema CONSTRAINT_TABLESCHEMA = new TableSchema(new String[] {"constrain"},
		new TypeInformation[] {Types.STRING});

	public BinningTrainForScorecardBatchOp() {
	}

	public BinningTrainForScorecardBatchOp(Params params) {
		super(params);
	}

	@Override
	public BinningTrainForScorecardBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		BinningTrainBatchOp op = new BinningTrainBatchOp(this.getParams()).linkFrom(in);

		this.setOutput(op.getDataSet(), op.getSchema());

		String constraint = getParams().get(HasConstraint.CONSTRAINT);
		constraint = null == constraint ? "" : constraint;

		DataSet <Row> dataSet = MLEnvironmentFactory
			.get(this.get(HasMLEnvironmentId.ML_ENVIRONMENT_ID))
			.getExecutionEnvironment()
			.fromElements(constraint)
			.map(new MapFunction <String, Row>() {
				private static final long serialVersionUID = 9023058389801218418L;

				@Override
				public Row map(String value) throws Exception {
					return Row.of(value);
				}
			});

		this.setSideOutputTables(new Table[] {
			DataSetConversionUtil.toTable(this.getMLEnvironmentId(),
				dataSet, CONSTRAINT_TABLESCHEMA)});

		return this;
	}
}
