package com.alibaba.alink.operator.stream.recommendation;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.ReservedColsWithSecondInputSpec;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.io.directreader.DataBridge;
import com.alibaba.alink.common.io.directreader.DirectReader;
import com.alibaba.alink.common.model.DataBridgeModelSource;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.FourFunction;
import com.alibaba.alink.operator.common.recommendation.RecommAdapter;
import com.alibaba.alink.operator.common.recommendation.RecommAdapterMT;
import com.alibaba.alink.operator.common.recommendation.RecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommMapper;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.operator.common.modelstream.ModelStreamUtils;
import com.alibaba.alink.operator.stream.utils.PredictProcess;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.ModelStreamFileSourceStreamOp;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.ModelStreamScanParams;
import com.alibaba.alink.params.mapper.ModelMapperParams;

/**
 * Base recommendation stream op.
 */

@Internal
@InputPorts(values = {
	@PortSpec(value = PortType.MODEL, opType = OpType.BATCH),
	@PortSpec(PortType.DATA),
	@PortSpec(value = PortType.MODEL_STREAM, isOptional = true)
})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ReservedColsWithSecondInputSpec
@NameCn("推荐基类")
public class BaseRecommStreamOp<T extends BaseRecommStreamOp <T>> extends StreamOperator <T>
	implements ModelStreamScanParams <T> {

	private static final long serialVersionUID = 5293170481337594373L;
	private final BatchOperator <?> model;

	/**
	 * (modelScheme, dataSchema, params) -> RecommKernel
	 */
	private final FourFunction <TableSchema, TableSchema, Params, RecommType, RecommKernel> recommKernelBuilder;
	private final RecommType recommType;

	public BaseRecommStreamOp(
		BatchOperator <?> model,
		FourFunction <TableSchema, TableSchema, Params, RecommType, RecommKernel> recommKernelBuilder,
		RecommType recommType,
		Params params) {

		super(params);
		this.model = model;
		this.recommKernelBuilder = recommKernelBuilder;
		this.recommType = recommType;
	}

	@Override
	public T linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = checkAndGetFirst(inputs);

		TableSchema modelSchema = this.model.getSchema();

		try {
			DataBridge modelDataBridge = DirectReader.collect(model);
			DataBridgeModelSource modelSource = new DataBridgeModelSource(modelDataBridge);
			RecommMapper mapper =
				new RecommMapper(
					this.recommKernelBuilder, this.recommType,
					modelSchema,
					in.getSchema(), this.getParams()
				);
			DataStream <Row> resultRows;
			DataStream <Row> modelStream = null;
			TableSchema modelStreamSchema = null;

			if (ModelStreamUtils.useModelStreamFile(getParams())) {
				StreamOperator <?> modelStreamOp = new ModelStreamFileSourceStreamOp()
					.setFilePath(getModelStreamFilePath())
					.setScanInterval(getModelStreamScanInterval())
					.setStartTime(getModelStreamStartTime())
					.setSchemaStr(TableUtil.schema2SchemaStr(modelSchema))
					.setMLEnvironmentId(getMLEnvironmentId());
				modelStreamSchema = modelStreamOp.getSchema();
				modelStream = modelStreamOp.getDataStream();
			}

			if (inputs.length > 1) {
				StreamOperator <?> localModelStreamOp = inputs[1];

				if (modelStream == null) {
					modelStreamSchema = localModelStreamOp.getSchema();
					modelStream = localModelStreamOp.getDataStream();
				} else {
					localModelStreamOp = localModelStreamOp.select(modelStreamSchema.getFieldNames());
					modelStream = modelStream.union(localModelStreamOp.getDataStream());
				}
			}

			if (modelStream != null) {
				resultRows = in
					.getDataStream()
					.connect(ModelMapStreamOp.broadcastStream(modelStream))
					.flatMap(
						new PredictProcess(
							modelSchema, in.getSchema(), getParams(), recommKernelBuilder, recommType, modelDataBridge,
							ModelStreamUtils.findTimestampColIndexWithAssertAndHint(modelStreamSchema),
							ModelStreamUtils.findCountColIndexWithAssertAndHint(modelStreamSchema)
						)
					);
			} else if (getParams().get(ModelMapperParams.NUM_THREADS) <= 1) {
				resultRows = in.getDataStream().map(new RecommAdapter(mapper, modelSource));
			} else {
				resultRows = in.getDataStream().flatMap(
					new RecommAdapterMT(mapper, modelSource, getParams().get(ModelMapperParams.NUM_THREADS))
				);
			}

			TableSchema outputSchema = mapper.getOutputSchema();

			this.setOutput(resultRows, outputSchema);

			return (T) this;
		} catch (Exception ex) {
			throw new AkUnclassifiedErrorException(ex.toString());
		}
	}

}
