package com.alibaba.alink.operator.common.feature.pca;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.feature.HasCalculationType;
import com.alibaba.alink.params.feature.PcaPredictParams;

import java.util.Arrays;
import java.util.List;

/**
 * Predictor for pca which will be used for stream and batch predict
 */
public class PcaModelMapper extends ModelMapper {

	private static final long serialVersionUID = -6656670267982283314L;
	private PcaModelData model = null;

	private int[] featureIdxs = null;
	private boolean isVector;

	private HasCalculationType.CalculationType pcaType = null;

	private double[] sourceMean = null;
	private double[] sourceStd = null;

	public PcaModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	private int[] checkGetColIndices(Boolean isVector, String[] featureColNames, String vectorColName) {
		String[] colNames = getDataSchema().getFieldNames();
		if (!isVector) {
			TableUtil.assertSelectedColExist(colNames, featureColNames);
			TableUtil.assertNumericalCols(getDataSchema(), featureColNames);
			return TableUtil.findColIndicesWithAssertAndHint(colNames, featureColNames);
		} else {
			TableUtil.assertSelectedColExist(colNames, vectorColName);
			TableUtil.assertVectorCols(getDataSchema(), vectorColName);
			return new int[] {TableUtil.findColIndexWithAssertAndHint(colNames, vectorColName)};
		}
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		model = new PcaModelDataConverter().load(modelRows);

		String[] featureColNames = model.featureColNames;
		String vectorColName = model.vectorColName;

		if (params.contains(PcaPredictParams.VECTOR_COL)) {
			vectorColName = params.get(PcaPredictParams.VECTOR_COL);
		}

		if (vectorColName != null) {
			this.isVector = true;
		}

		this.featureIdxs = checkGetColIndices(isVector, featureColNames, vectorColName);
		this.pcaType = model.pcaType;
		int nx = model.means.length;

		//transform mean, stdDevs and scoreStd
		sourceMean = new double[nx];
		sourceStd = new double[nx];
		Arrays.fill(sourceStd, 1);

		if (HasCalculationType.CalculationType.CORR == this.pcaType) {
			sourceStd = model.stddevs;
			sourceMean = model.means;
		}
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema modelSchema,
																						   TableSchema dataSchema,
																						   Params params) {
		return Tuple4.of(dataSchema.getFieldNames(),
			new String[] {params.get(PcaPredictParams.PREDICTION_COL)},
			new TypeInformation[] {VectorTypes.DENSE_VECTOR},
			params.get(PcaPredictParams.RESERVED_COLS));
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		//transform data
		double[] data = new double[this.model.nx];
		if (isVector) {
			Vector parsed = VectorUtil.getVector(selection.get(featureIdxs[0]));
			if (parsed instanceof SparseVector) {
				if (parsed.size() < 0) {
					((SparseVector) parsed).setSize(model.nx);
				}
			}
			for (int i = 0; i < parsed.size(); i++) {
				data[i] = parsed.get(i);
			}
		} else {
			for (int i = 0; i < this.featureIdxs.length; ++i) {
				data[i] = (Double) selection.get(this.featureIdxs[i]);
			}
		}

		double[] predictData;
		if (model.idxNonEqual.length != data.length) {
			Integer[] indices = model.idxNonEqual;
			double[] dataNe = new double[indices.length];
			for (int i = 0; i < indices.length; i++) {
				if (Math.abs(this.sourceStd[i]) > 1e-12) {
					int idx = indices[i];
					dataNe[i] = (data[idx] - this.sourceMean[i]) / this.sourceStd[i];
				}
			}
			predictData = model.calcPrinValue(dataNe);
		} else {
			for (int i = 0; i < data.length; i++) {
				if (Math.abs(this.sourceStd[i]) > 1e-12) {
					data[i] = (data[i] - this.sourceMean[i]) / this.sourceStd[i];
				}
			}
			predictData = model.calcPrinValue(data);
		}

		result.set(0, new DenseVector(predictData));
	}
}
