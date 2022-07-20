package com.alibaba.alink.operator.common.tensorflow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.mapper.FlatModelMapper;
import com.alibaba.alink.common.mapper.RichModelMapper;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.params.mapper.RichModelMapperParams;

/**
 * Abstract class for mappers with model with inputs cached.
 * <p>
 * Like {@link RichModelMapper}, outputs rows and inputs rows are still one-to-one correspondence, but inputs are cached
 * for batch processing for better performance, i.e. inferences in TensorFlow or PyTorch.
 *
 * <p>The RichModel is used to the classification, the regression or the clustering.
 * The output of the model mapper using RichModel as its model contains three part:
 * <ul>
 * <li>The reserved columns from input</li>
 * <li>The prediction result column</li>
 * <li>The prediction detail column</li>
 * </ul>
 * <p>
 * For post-processing of concatenation of reserved columns and result columns, {@link PredictionCollector} is used
 * to store input rows, and complete the concatenation when collecting.
 */
public abstract class CachedRichModelMapper extends FlatModelMapper {

	private static final long serialVersionUID = -6722995426402759862L;
	/**
	 * The output column helper which control the output format.
	 *
	 * @see OutputColsHelper
	 */
	private final OutputColsHelper outputColsHelper;

	/**
	 * The condition that the mapper output the prediction detail or not.
	 */
	private final boolean isPredDetail;

	public CachedRichModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		String[] reservedColNames = this.params.get(RichModelMapperParams.RESERVED_COLS);
		String predResultColName = this.params.get(RichModelMapperParams.PREDICTION_COL);
		TypeInformation predResultColType = initPredResultColType();
		isPredDetail = params.contains(RichModelMapperParams.PREDICTION_DETAIL_COL);
		if (isPredDetail) {
			String predDetailColName = params.get(RichModelMapperParams.PREDICTION_DETAIL_COL);
			this.outputColsHelper = new OutputColsHelper(dataSchema,
				new String[] {predResultColName, predDetailColName},
				new TypeInformation[] {predResultColType, Types.STRING}, reservedColNames);
		} else {
			this.outputColsHelper = new OutputColsHelper(dataSchema, predResultColName, predResultColType,
				reservedColNames);
		}
	}

	/**
	 * Cache origin input row, and do concatenation when collect is called. The final output row is then fed to wrapped
	 * {@link #collector}.
	 * <p>
	 * NOTE: The caller should ensure {@link #input} not be changed outside.
	 */
	protected class PredictionCollector implements Collector <Row> {
		private final Row input;
		private final Collector <Row> collector;

		public PredictionCollector(Row input, Collector <Row> collector) {
			this.input = input;
			this.collector = collector;
		}

		@Override
		public void collect(Row row) {
			try {
				if (isPredDetail) {
					Tuple2 <Object, String> t2 = extractPredictResultDetail(row);
					collector.collect(outputColsHelper.getResultRow(input, Row.of(t2.f0, t2.f1)));
				} else {
					collector.collect(
						outputColsHelper.getResultRow(input, Row.of(extractPredictResult(row))));
				}
			} catch (Exception e) {
				throw new AkUnclassifiedErrorException("Failed to extract or concatenate predictions.", e);
			}
		}

		@Override
		public void close() {
			this.collector.close();
		}
	}

	/**
	 * Initial the prediction result column type.
	 *
	 * <p>The subclass can override this method to initial the {@link OutputColsHelper}
	 *
	 * @return the type of the prediction result column
	 */
	protected TypeInformation initPredResultColType() {
		return super.getModelSchema().getFieldTypes()[2];
	}

	@Override
	public TableSchema getOutputSchema() {
		return outputColsHelper.getResultSchema();
	}

	/**
	 * Calculate the prediction result.
	 *
	 * @param row the input
	 * @return the prediction result.
	 */
	protected abstract Object extractPredictResult(Row row) throws Exception;

	/**
	 * Calculate the prediction result ant the prediction detail.
	 *
	 * @param row the input
	 * @return The prediction result and the the prediction detail.
	 */
	protected abstract Tuple2 <Object, String> extractPredictResultDetail(Row row) throws Exception;

	@Override
	public void flatMap(Row row, Collector <Row> output) throws Exception {
		if (isPredDetail) {
			Tuple2 <Object, String> t2 = extractPredictResultDetail(row);
			output.collect(outputColsHelper.getResultRow(row, Row.of(t2.f0, t2.f1)));
		} else {
			output.collect(outputColsHelper.getResultRow(row, Row.of(extractPredictResult(row))));
		}
	}
}
