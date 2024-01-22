package com.alibaba.alink.operator.local.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.ReservedColsWithFirstInputSpec;
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.feature.BaseCrossTrainBatchOp;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelData;
import com.alibaba.alink.operator.common.feature.OneHotModelDataConverter;
import com.alibaba.alink.operator.common.feature.OneHotModelMapper;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.TableSourceLocalOp;
import com.alibaba.alink.params.classification.RandomForestTrainParams;
import com.alibaba.alink.params.feature.AutoCrossTrainParams;
import com.alibaba.alink.params.feature.HasDropLast;
import com.alibaba.alink.params.shared.colname.HasOutputColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.TransformerBase;
import com.alibaba.alink.pipeline.feature.AutoCrossAlgoModel;
import com.alibaba.alink.pipeline.feature.OneHotEncoderModel;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@ReservedColsWithFirstInputSpec
@ParamSelectColumnSpec(name = "selectedCols")
@ParamSelectColumnSpec(name = "labelCol")
@NameCn("")
abstract class BaseCrossTrainLocalOp<T extends BaseCrossTrainLocalOp <T>>
	extends LocalOperator <T>
	implements AutoCrossTrainParams <T> {

	static final String oneHotVectorCol = "oneHotVectorCol";

	BaseCrossTrainLocalOp(Params params) {
		super(params);
	}

	//todo construct function.

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);

		String[] reversedCols = getParams().get(HasReservedColsDefaultAsNull.RESERVED_COLS);
		if (reversedCols == null) {
			reversedCols = in.getColNames();
		}

		String[] featureCols = getSelectedCols();
		final String labelCol = getLabelCol();

		String[] selectedCols = ArrayUtils.add(featureCols, labelCol);
		in = in.select(selectedCols);
		TableSchema inputSchema = in.getSchema();

		String[] categoricalCols = TableUtil.getCategoricalCols(in.getSchema(), featureCols,
			getParams().contains(RandomForestTrainParams.CATEGORICAL_COLS) ?
				getParams().get(RandomForestTrainParams.CATEGORICAL_COLS) : null
		);
		if (null == categoricalCols || categoricalCols.length == 0) {
			throw new AkIllegalArgumentException("Please input param CategoricalCols!");
		}
		String[] numericalCols = ArrayUtils.removeElements(featureCols, categoricalCols);

		Params oneHotParams = new Params().set(AutoCrossTrainParams.SELECTED_COLS, categoricalCols);
		if (getParams().contains(AutoCrossTrainParams.DISCRETE_THRESHOLDS_ARRAY)) {
			oneHotParams.set(AutoCrossTrainParams.DISCRETE_THRESHOLDS_ARRAY, getDiscreteThresholdsArray());
		} else if (getParams().contains(AutoCrossTrainParams.DISCRETE_THRESHOLDS)) {
			oneHotParams.set(AutoCrossTrainParams.DISCRETE_THRESHOLDS, getDiscreteThresholds());
		}
		oneHotParams.set(HasDropLast.DROP_LAST, false)
			.set(HasOutputColsDefaultAsNull.OUTPUT_COLS, new String[] {oneHotVectorCol});

		OneHotTrainLocalOp oneHotModel = new OneHotTrainLocalOp(oneHotParams)
			.linkFrom(in);

		OneHotEncoderModel oneHotEncoderModel = new OneHotEncoderModel(oneHotParams);
		oneHotEncoderModel.setModelData(oneHotModel);

		TransformerBase <?>[] finalModel = new TransformerBase[2];
		finalModel[0] = oneHotEncoderModel;

		in = new OneHotPredictLocalOp(oneHotParams)
			.linkFrom(oneHotModel, in);

		boolean enableElse = OneHotModelMapper.isEnableElse(oneHotParams);

		int additionalSize = enableElse ? 2 : 1;

		MultiStringIndexerModelData onehotModelData = new OneHotModelDataConverter().load(
			oneHotModel.getOutput().getRows()).modelData;
		int featureNumber = onehotModelData.tokenNumber.size();
		int[] featureSize = new int[featureNumber];
		for (int i = 0; i < featureNumber; i++) {
			featureSize[i] = (int) (onehotModelData.tokenNumber.get(i) + additionalSize);
		}

		List <Object> positiveLabel = new ArrayList <>();
		for (Row row : in.select(labelCol).distinct().getOutput().getRows()) {
			positiveLabel.add(row.getField(0));
		}

		int svIndex = TableUtil.findColIndex(in.getColNames(), oneHotVectorCol);
		int labelIndex = TableUtil.findColIndex(in.getColNames(), labelCol);

		int[] numericalIndices = TableUtil.findColIndicesWithAssert(in.getSchema(), numericalCols);

		DataColumnsSaver dataColumnsSaver = new DataColumnsSaver(categoricalCols, numericalCols, numericalIndices);

		//here numerical cols is concatted first.
		List <Tuple2 <Integer, Tuple3 <Double, Double, Vector>>> trainDataOrigin = new ArrayList <>();
		if (true) {
			SparseVector sv1 = VectorUtil.getSparseVector(in.getOutputTable().getEntry(0, svIndex));
			int vecSize = numericalIndices.length + sv1.getIndices().length;
			int svSize = numericalIndices.length + sv1.size();
			for (Row rowData : in.getOutputTable().getRows()) {
				int[] vecIndices = new int[vecSize];
				double[] vecValues = new double[vecSize];
				for (int i = 0; i < numericalIndices.length; i++) {
					vecIndices[i] = i;
					vecValues[i] = ((Number) rowData.getField(numericalIndices[i])).doubleValue();
				}
				SparseVector sv = VectorUtil.getSparseVector(rowData.getField(svIndex));
				int[] svIndices = new int[sv.getIndices().length];
				for (int i = 0; i < svIndices.length; i++) {
					svIndices[i] = sv.getIndices()[i] + numericalIndices.length;
				}
				System.arraycopy(svIndices, 0, vecIndices, numericalIndices.length, sv.getIndices().length);
				System.arraycopy(sv.getValues(), 0, vecValues, numericalIndices.length, sv.getValues().length);

				trainDataOrigin.add(Tuple2.of(0, Tuple3.of(
					1.0,
					positiveLabel.get(0).equals(rowData.getField(labelIndex)) ? 1. : 0.,
					new SparseVector(svSize, vecIndices, vecValues))));
			}
		}

		List <Tuple3 <Double, Double, Vector>> trainData = new ArrayList <>();
		for (Tuple2 <Integer, Tuple3 <Double, Double, Vector>> value : trainDataOrigin) {
			trainData.add(value.f1);
		}

		List <Row> acModel = buildAcModelData(trainData, featureSize, dataColumnsSaver);

		LocalOperator <?> autoCrossBatchModel = new TableSourceLocalOp(
			new MTable(acModel, new String[] {"feature_id", "cross_feature", "score"},
				new TypeInformation[] {Types.LONG, Types.STRING, Types.DOUBLE})
		);

		Params autoCrossParams = getParams();
		autoCrossParams.set(HasReservedColsDefaultAsNull.RESERVED_COLS, reversedCols);
		AutoCrossAlgoModel acPipelineModel = new AutoCrossAlgoModel(autoCrossParams)
			.setModelData(autoCrossBatchModel);
		finalModel[1] = acPipelineModel;

		LocalOperator <?> modelSaved = new PipelineModel(finalModel).saveLocal();

		List <Row> modelRows = new ArrayList <>();
		int extendLen = selectedCols.length + 1;
		for (Row value : modelSaved.getOutputTable().getRows()) {
			Row out = new Row(value.getArity() + extendLen);
			for (int i = 0; i < value.getArity(); i++) {
				out.setField(i, value.getField(i));
			}
			modelRows.add(out);
		}

		setOutputTable(new MTable(modelRows,
			BaseCrossTrainBatchOp.getAutoCrossModelSchema(inputSchema, modelSaved.getSchema(), selectedCols)));

		buildSideOutput(oneHotModel, acModel, Arrays.asList(numericalCols));
	}

	abstract List <Row> buildAcModelData(List <Tuple3 <Double, Double, Vector>> trainData,
										 int[] featureSize,
										 DataColumnsSaver dataColumnsSaver);

	abstract void buildSideOutput(OneHotTrainLocalOp oneHotModel, List <Row> acModel,
								  List <String> numericalCols);

	static class DataColumnsSaver {
		String[] categoricalCols;
		String[] numericalCols;
		int[] numericalIndices;

		DataColumnsSaver(String[] categoricalCols,
						 String[] numericalCols,
						 int[] numericalIndices) {
			this.categoricalCols = categoricalCols;
			this.numericalCols = numericalCols;
			this.numericalIndices = numericalIndices;
		}
	}

}
