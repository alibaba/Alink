package com.alibaba.alink.pipeline.feature;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.common.feature.AutoCross.AutoCrossModelMapper;
import com.alibaba.alink.operator.common.feature.AutoCross.BuildSideOutput;
import com.alibaba.alink.params.feature.AutoCrossPredictParams;
import com.alibaba.alink.pipeline.MapModel;
import com.alibaba.alink.pipeline.ModelExporterUtils;
import com.alibaba.alink.pipeline.PipelineStageBase;

import java.util.ArrayList;
import java.util.List;

@NameCn("Auto Cross模型")
public class AutoCrossModel extends MapModel <AutoCrossModel>
	implements AutoCrossPredictParams <AutoCrossModel> {

	private static final long serialVersionUID = -901650815591602025L;

	public AutoCrossModel() {
		this(new Params());
	}

	public AutoCrossModel(Params params) {
		super(AutoCrossModelMapper::new, params);
	}

	public BatchOperator <?> getCrossInformation() {
		final String[] dataFieldNames = getModelData().getSchema().getFieldNames();
		final DataType[] dataFieldTypes = getModelData().getSchema().getFieldDataTypes();

		DataSet <Row> crossInfoDataSet = this.getModelData()
			.getDataSet()
			.mapPartition(new MapPartitionFunction <Row, Row>() {
				private static final long serialVersionUID = 7224060248191059567L;

				@Override
				public void mapPartition(Iterable <Row> values, Collector <Row> out) throws Exception {
					List <Row> pipelineModels = new ArrayList <>();
					for (Row row : values) {
						pipelineModels.add(row);
					}

					List <Row> oneHotModel;
					List <Row> autoCrossModel;

					TableSchema modelSchema = TableSchema.builder().fields(dataFieldNames, dataFieldTypes).build();

					List <Tuple3 <PipelineStageBase <?>, TableSchema, List <Row>>> deserialized =
						ModelExporterUtils.loadStagesFromPipelineModel(pipelineModels, modelSchema);

					oneHotModel = deserialized.get(0).f2;
					autoCrossModel = deserialized.get(1).f2;

					BuildSideOutput.buildModel(oneHotModel, autoCrossModel, out);
				}
			}).setParallelism(1);
		Table crossInfo = DataSetConversionUtil.toTable(this.getModelData().getMLEnvironmentId(), crossInfoDataSet,
			new String[] {"index", "feature", "value"},
			new TypeInformation[] {Types.INT, Types.STRING, Types.STRING});
		return new TableSourceBatchOp(crossInfo);
	}

}
