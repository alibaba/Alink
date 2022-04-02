package com.alibaba.alink.operator.stream.onlinelearning;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.function.TriFunction;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.ReservedColsWithSecondInputSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.io.directreader.DataBridge;
import com.alibaba.alink.common.io.directreader.DirectReader;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.stream.model.ModelStreamUtils;
import com.alibaba.alink.operator.common.stream.model.PredictProcess;
import com.alibaba.alink.operator.stream.StreamOperator;

/**
 */

@Internal
@InputPorts(values = {
	@PortSpec(value = PortType.MODEL, opType = OpType.BATCH),
	@PortSpec(value = PortType.MODEL_STREAM, opType = OpType.SAME),
	@PortSpec(value = PortType.DATA, opType = OpType.SAME),
})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@ReservedColsWithSecondInputSpec
@NameCn("在线学习预测基类")
public class BaseOnlinePredictStreamOp<T extends BaseOnlinePredictStreamOp <T>> extends StreamOperator <T> {

	DataBridge dataBridge = null;
	private final BatchOperator<?> model;

	/**
	 * (modelScheme, dataSchema, params) -> ModelMapper
	 */
	private final TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder;

	public BaseOnlinePredictStreamOp(BatchOperator<?> model,
									 TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder,
									 Params params) {
		super(params);
		this.model = model;
		this.mapperBuilder = mapperBuilder;
	}

	@Override
	public T linkFrom(StreamOperator <?>... inputs) {
		checkOpSize(2, inputs);

		try {
			if (model != null) {
				dataBridge = DirectReader.collect(model);
			} else {
				throw new IllegalArgumentException(
					"online algo: initial model is null. Please set a valid initial model.");
			}

			DataStream <Row> modelstr = ModelStreamUtils.broadcastStream(inputs[0].getDataStream());

			TypeInformation<?>[] types = new TypeInformation[3];
			String[] names = new String[3];
			for (int i = 0; i < 3; ++i) {
				names[i] = inputs[0].getSchema().getFieldNames()[i + 2];
				types[i] = inputs[0].getSchema().getFieldTypes()[i + 2];
			}
			TableSchema modelSchema = new TableSchema(names, types);
			/* predict samples */
			DataStream <Row> prediction = inputs[1].getDataStream()
				.connect(modelstr)
				.flatMap(
					new PredictProcess(modelSchema, inputs[1].getSchema(), this.getParams(), mapperBuilder, dataBridge, 0, 1)
				);

			this.setOutputTable(DataStreamConversionUtil.toTable(getMLEnvironmentId(), prediction,
				mapperBuilder.apply(modelSchema, inputs[1].getSchema(), getParams()).getOutputSchema()));

		} catch (Exception ex) {
			ex.printStackTrace();
			throw new RuntimeException(ex.toString());
		}

		return (T) this;
	}
}
