package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.function.TriFunction;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.outlier.HasDetectLast;
import com.alibaba.alink.params.outlier.HasInputMTableCol;
import com.alibaba.alink.params.outlier.HasOutputMTableCol;
import com.alibaba.alink.params.outlier.HasWithSeriesInfo;

@NameCn("异常检测基类")
@Internal
public class BaseModelOutlierWithSeriesPredictBatchOp<T extends BaseModelOutlierWithSeriesPredictBatchOp <T>>
	extends ModelMapBatchOp <T> implements ModelOutlierWithSeriesDetectorParams <T> {

	public BaseModelOutlierWithSeriesPredictBatchOp(
		TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder,
		Params params) {
		super(mapperBuilder, params);
	}

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		checkOpSize(2, inputs);

		try {
			if (getParams().get(HasWithSeriesInfo.WITH_SERIES_INFO)) {
				//Step 1 : Grouped the input rows into MTables
				BatchOperator <?> in_grouped = BaseOutlierBatchOp.group2MTables(inputs[1], getParams());

				//Step 2 : detect the outlier for each MTable
				ModelMapper mapper = this.mapperBuilder.apply(inputs[0].getSchema(), inputs[1].getSchema(),
					getParams().clone()
						.set(HasInputMTableCol.INPUT_MTABLE_COL, OutlierDetector.TEMP_MTABLE_COL)
						.set(HasOutputMTableCol.OUTPUT_MTABLE_COL, OutlierDetector.TEMP_MTABLE_COL)
						.set(HasDetectLast.DETECT_LAST, false)
				);
				DataSet <Row> resultRows = ModelMapBatchOp.calcResultRows(inputs[0], in_grouped, mapper, getParams());

				//Step 3 : Flatten the MTables to final results
				Table resultTable = BaseOutlierBatchOp.flattenMTable(
					resultRows, inputs[1].getSchema(), mapper.getOutputSchema(), getParams(), getMLEnvironmentId()
				);

				this.setOutputTable(resultTable);
			} else {
				final ModelMapper mapper = this.mapperBuilder.apply(
					inputs[0].getSchema(),
					inputs[1].getSchema(),
					this.getParams());

				DataSet <Row> resultRows = ModelMapBatchOp.calcResultRows(inputs[0], inputs[1], mapper, getParams());

				TableSchema outputSchema = mapper.getOutputSchema();
				this.setOutput(resultRows, outputSchema);
			}

			return (T) this;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

}