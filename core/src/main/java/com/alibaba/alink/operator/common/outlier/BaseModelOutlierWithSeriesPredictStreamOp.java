package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.function.TriFunction;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.directreader.DataBridge;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.io.ModelFileSinkParams;
import com.alibaba.alink.params.outlier.HasDetectLast;
import com.alibaba.alink.params.outlier.HasInputMTableCol;
import com.alibaba.alink.params.outlier.HasOutputMTableCol;
import com.alibaba.alink.params.outlier.HasWithSeriesInfo;
import com.alibaba.alink.params.shared.HasModelFilePath;

@NameCn("异常检测基类")
public class BaseModelOutlierWithSeriesPredictStreamOp<T extends BaseModelOutlierWithSeriesPredictStreamOp <T>>
	extends ModelMapStreamOp <T> implements ModelOutlierWithSeriesDetectorParams <T>, HasModelFilePath<T> {

	public BaseModelOutlierWithSeriesPredictStreamOp(BatchOperator <?> model,
													 TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder,
													 Params params) {
		super(model, mapperBuilder, params);
	}

	@Override
	public T linkFrom(StreamOperator <?>... inputs) {
		checkMinOpSize(1, inputs);

		StreamOperator <?> input_data = inputs[0];

		StreamOperator <?> input_model_stream = inputs.length > 1 ? inputs[1] : null;

		try {
			if (getParams().get(HasWithSeriesInfo.WITH_SERIES_INFO)) {
				//Step 1 : Grouped the input rows into MTables
				StreamOperator <?> in_grouped = BaseOutlierStreamOp.group2MTables(input_data, getParams());

				Tuple2 <DataBridge, TableSchema> dataBridge = createDataBridge(
					getParams().get(ModelFileSinkParams.MODEL_FILE_PATH),
					model
				);

				//Step 2 : detect the outlier for each MTable
				ModelMapper mapper = this.mapperBuilder.apply(dataBridge.f1, input_data.getSchema(),
					getParams().clone()
						.set(HasInputMTableCol.INPUT_MTABLE_COL, OutlierDetector.TEMP_MTABLE_COL)
						.set(HasOutputMTableCol.OUTPUT_MTABLE_COL, OutlierDetector.TEMP_MTABLE_COL)
						.set(HasDetectLast.DETECT_LAST, false)
				);
				DataStream <Row> resultRows = ModelMapStreamOp.calcResultRows(
					dataBridge.f0, dataBridge.f1, in_grouped, input_model_stream,
					mapper, getParams(), getMLEnvironmentId(), mapperBuilder);

				//Step 3 : Flatten the MTables to final results
				Table resultTable = BaseOutlierStreamOp.flattenMTable(
					resultRows, input_data.getSchema(), mapper.getOutputSchema(), getParams(), getMLEnvironmentId()
				);

				this.setOutputTable(resultTable);
			} else {

				Tuple2 <DataBridge, TableSchema> dataBridge = createDataBridge(
					getParams().get(ModelFileSinkParams.MODEL_FILE_PATH),
					model
				);

				final ModelMapper mapper = this.mapperBuilder.apply(dataBridge.f1, input_data.getSchema(),
					this.getParams());

				DataStream <Row> resultRows = ModelMapStreamOp.calcResultRows(
					dataBridge.f0, dataBridge.f1, input_data, input_model_stream,
					mapper, getParams(), getMLEnvironmentId(), mapperBuilder);

				TableSchema outputSchema = mapper.getOutputSchema();
				this.setOutput(resultRows, outputSchema);
			}

			return (T) this;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

}