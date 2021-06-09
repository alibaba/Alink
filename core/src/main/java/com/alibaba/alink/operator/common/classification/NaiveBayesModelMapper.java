package com.alibaba.alink.operator.common.classification;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.BLAS;
import com.alibaba.alink.common.mapper.RichModelMapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelMapper;
import com.alibaba.alink.operator.common.dataproc.NumericalTypeCastMapper;
import com.alibaba.alink.params.dataproc.HasHandleInvalid;
import com.alibaba.alink.params.dataproc.MultiStringIndexerPredictParams;
import com.alibaba.alink.params.dataproc.NumericalTypeCastParams;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This mapper predicts sample label.
 */
public class NaiveBayesModelMapper extends RichModelMapper {

	private static final long serialVersionUID = -1139163925664147812L;
	private int[] featureIndices;
	private NaiveBayesModelData modelData;
	private MultiStringIndexerModelMapper stringIndexerModelPredictor;
	private NumericalTypeCastMapper stringIndexerModelNumericalTypeCastMapper;
	protected NumericalTypeCastMapper numericalTypeCastMapper;
	private boolean getCate;
	private final double constant = 0.5 * Math.log(2 * Math.PI);
	/*
	 * If the ratio of data variance between dimensions is too small, it
	 * will cause numerical errors. To address this, we modify the probability
	 * of those data.
	 */
	private final double maxValue = 0.;
	private final double minValue = Math.log(1e-9);
	private transient ThreadLocal<Row> inputBufferThreadLocal;

	public NaiveBayesModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	@Override
	protected Object predictResult(SlicedSelectedSample selection) throws Exception {
		Row inputBuffer = inputBufferThreadLocal.get();
		selection.fillRow(inputBuffer);
		double[] probs = calculateProb(inputBuffer);
		return NaiveBayesTextModelMapper.findMaxProbLabel(probs, modelData.label);
	}

	@Override
	protected Tuple2 <Object, String> predictResultDetail(SlicedSelectedSample selection) throws Exception {
		Row inputBuffer = inputBufferThreadLocal.get();
		selection.fillRow(inputBuffer);
		double[] probs = calculateProb(inputBuffer);
		Object label = NaiveBayesTextModelMapper.findMaxProbLabel(probs, modelData.label);
		String jsonDetail = NaiveBayesTextModelMapper.generateDetail(probs, modelData.piArray, modelData.label);
		return Tuple2.of(label, jsonDetail);
	}


	@Override
	public void loadModel(List <Row> modelRows) {
		this.modelData = new NaiveBayesModelDataConverter().load(modelRows);
		int featureNumber = modelData.featureNames.length;
		featureIndices = new int[featureNumber];
		for (int i = 0; i < featureNumber; ++i) {
			featureIndices[i] = TableUtil.findColIndex(ioSchema.f0, modelData.featureNames[i]);
		}
		TableSchema modelSchema = getModelSchema();
		List <String> listCateCols = new ArrayList <>();
		for (int i = 0; i < featureNumber; i++) {
			if (modelData.isCate[i]) {
				listCateCols.add(modelData.featureNames[i]);
			}
		}
		String[] categoricalColNames = listCateCols.toArray(new String[0]);
		getCate = categoricalColNames.length != 0;
		if (getCate) {
			stringIndexerModelPredictor = new MultiStringIndexerModelMapper(
				modelSchema,
				getDataSchema(),
				new Params()
					.set(HasSelectedCols.SELECTED_COLS, categoricalColNames)
					.set(
						MultiStringIndexerPredictParams.HANDLE_INVALID,
						HasHandleInvalid.HandleInvalid.SKIP
					)
			);
			stringIndexerModelPredictor.loadModel(modelData.stringIndexerModelSerialized);

			stringIndexerModelNumericalTypeCastMapper = new NumericalTypeCastMapper(getDataSchema(),
				new Params()
					.set(NumericalTypeCastParams.SELECTED_COLS, categoricalColNames)
					.set(NumericalTypeCastParams.TARGET_TYPE, NumericalTypeCastParams.TargetType.valueOf("INT"))
			);
		}

		numericalTypeCastMapper = new NumericalTypeCastMapper(getDataSchema(),
			new Params()
				.set(
					NumericalTypeCastParams.SELECTED_COLS,
					ArrayUtils.removeElements(modelData.featureNames, categoricalColNames)//to check col Names.
				)
				.set(NumericalTypeCastParams.TARGET_TYPE, NumericalTypeCastParams.TargetType.valueOf("DOUBLE"))
		);
		inputBufferThreadLocal = ThreadLocal.withInitial(() -> new Row(ioSchema.f0.length));
	}

	private Row transRow(Row row) throws Exception {
		if (getCate) {
			row = stringIndexerModelPredictor.map(row);
			row = stringIndexerModelNumericalTypeCastMapper.map(row);
		}

		return numericalTypeCastMapper.map(row);
	}

	/**
	 * Calculate probability of the input data.
	 */
	private double[] calculateProb(Row rowData) throws Exception {
		rowData = transRow(rowData);
		int labelSize = this.modelData.label.length;
		double[] probs = new double[labelSize];
		int featureSize = modelData.featureNames.length;
		int[] featureIndices = new int[featureSize];
		Arrays.fill(featureIndices, -1);
		boolean allZero = true;
		for (int i = 0; i < featureSize; i++) {
			if (rowData.getField(this.featureIndices[i]) != null) {
				featureIndices[i] = this.featureIndices[i];
				allZero = false;
			}
		}
		if (allZero) {
			double prob = 1. / labelSize;
			Arrays.fill(probs, prob);
			return probs;
		}
		for (int i = 0; i < labelSize; i++) {
			Number[][] labelData = modelData.theta[i];
			for (int j = 0; j < featureSize; j++) {
				int featureIndex = featureIndices[j];
				if (modelData.isCate[j]) {
					if (featureIndex != -1) {
						int index = (int) rowData.getField(featureIndex);
						if (index < labelData[j].length) {
							probs[i] += (Double) labelData[j][index];
						}
					}
				} else {
					double miu = (double) labelData[j][0];
					double sigma2 = (double) labelData[j][1];
					if (featureIndex == -1) {
						probs[i] -= (constant + 0.5 * Math.log(sigma2));
					} else {
						double data = (double) rowData.getField(featureIndex);
						if (sigma2 == 0) {
							if (Math.abs(data - miu) <= 1e-5) {
								probs[i] += maxValue;
							} else {
								probs[i] += minValue;
							}
						} else {
							double item1 = Math.pow(data - miu, 2) / (2 * sigma2);
							probs[i] -= (item1 + constant + 0.5 * Math.log(sigma2));
						}
					}
				}

			}
		}
		BLAS.axpy(1, modelData.piArray, probs);
		return probs;
	}
}
