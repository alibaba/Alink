package com.alibaba.alink.operator.common.dataproc.vector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.SISOModelMapper;
import com.alibaba.alink.common.model.RichModelDataConverter;
import com.alibaba.alink.params.dataproc.HasStrategy;
import com.alibaba.alink.params.dataproc.vector.VectorSrtPredictorParams;

import java.util.List;

/**
 * This mapper completes missing values in one vector data.
 * Strategy support min, max, mean or value.
 */
public class VectorImputerModelMapper extends SISOModelMapper {
	private static final long serialVersionUID = 961247156517825658L;
	private double[] values;
	private double value;

	public VectorImputerModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params.set(VectorSrtPredictorParams.SELECTED_COL,
			RichModelDataConverter.extractSelectedColNames(modelSchema)[0]));

	}

	@Override
	protected TypeInformation initPredResultColType() {
		return VectorTypes.VECTOR;
	}

	/**
	 * This function completes missing values in one Vector input data.
	 *
	 * @param input the input object, maybe sparse vector or dense vector
	 * @return the result of prediction
	 */
	@Override
	public Object predictResult(Object input) {
		Vector vec = VectorUtil.getVector(input);
		if (null == vec) {
			return null;
		} else if (vec instanceof DenseVector) {
			return predict((DenseVector) vec);
		} else {
			return predict((SparseVector) vec);
		}

	}

	/**
	 * Load model from the list of Row type data.
	 *
	 * @param modelRows the list of Row type data
	 */
	@Override
	public void loadModel(List <Row> modelRows) {
		VectorImputerModelDataConverter converter = new VectorImputerModelDataConverter();
		Tuple3 <HasStrategy.Strategy, double[], Double> tuple2 = converter.load(modelRows);
		this.values = tuple2.f1;
		if (this.values == null) {
			if (tuple2.f2 == null) {
				throw new RuntimeException("In VALUE strategy, the filling value is necessary.");
			}
			this.value = tuple2.f2;
		}
	}

	/**
	 * This function completes missing values in one DenseVector input data.
	 *
	 * @param vector the input dense vector
	 * @return vector without missing values
	 */
	private DenseVector predict(DenseVector vector) {
		double[] data = vector.getData();
		if (this.values != null) {
			for (int i = 0; i < data.length; i++) {
				if (Double.isNaN(data[i])) {
					data[i] = this.values[i];
				}
			}
		} else {
			for (int i = 0; i < data.length; i++) {
				if (Double.isNaN(data[i])) {
					data[i] = this.value;
				}
			}
		}
		return vector;
	}

	/**
	 * This function completes missing values in one SparseVector input data.
	 *
	 * @param vector the input sparse vector
	 * @return vector without missing values
	 */
	private SparseVector predict(SparseVector vector) {
		double[] vectorValues = vector.getValues();
		if (this.values != null) {
			for (int i = 0; i < vector.numberOfValues(); i++) {
				if (Double.isNaN(vectorValues[i])) {
					vectorValues[i] = this.values[vector.getIndices()[i]];
				}
			}
		} else {
			for (int i = 0; i < vector.numberOfValues(); i++) {
				if (Double.isNaN(vectorValues[i])) {
					vectorValues[i] = this.value;
				}
			}
		}
		return vector;
	}
}
