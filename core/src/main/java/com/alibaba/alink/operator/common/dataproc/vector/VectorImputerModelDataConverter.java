package com.alibaba.alink.operator.common.dataproc.vector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.model.RichModelDataConverter;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.alink.params.dataproc.vector.VectorImputerTrainParams.FILL_VALUE;
import static com.alibaba.alink.params.dataproc.vector.VectorImputerTrainParams.SELECTED_COL;
import static com.alibaba.alink.params.dataproc.vector.VectorImputerTrainParams.STRATEGY;
import static com.alibaba.alink.params.dataproc.vector.VectorImputerTrainParams.Strategy;

/**
 * This converter can help serialize and deserialize the model data.
 */
public class VectorImputerModelDataConverter extends
	RichModelDataConverter <Tuple3 <Strategy, BaseVectorSummary, Double>, Tuple3 <Strategy, double[], Double>> {

	public String vectorColName;

	/**
	 * Get the additional column names.
	 */
	@Override
	protected String[] initAdditionalColNames() {
		return new String[] {vectorColName};
	}

	/**
	 * Get the additional column types.
	 */
	@Override
	protected TypeInformation[] initAdditionalColTypes() {
		return new TypeInformation[] {Types.STRING};
	}

	/**
	 * Serialize the model data to "Tuple3<Params, List<String>, List<Row>>".
	 *
	 * @param modelData The model data to serialize.
	 * @return The serialization result.
	 */
	public Tuple3 <Params, Iterable <String>, Iterable <Row>> serializeModel(
		Tuple3 <Strategy, BaseVectorSummary, Double> modelData) {
		Strategy strategy = modelData.f0;
		BaseVectorSummary summary = modelData.f1;
		double fillValue = modelData.f2;
		double[] values = null;
		Params meta = new Params()
			.set(SELECTED_COL, vectorColName)
			.set(STRATEGY, strategy);
		switch (strategy) {
			case MIN:
				if (summary.min() instanceof DenseVector) {
					values = ((DenseVector) summary.min()).getData();
				} else {
					values = ((SparseVector) summary.min()).toDenseVector().getData();
				}
				break;
			case MAX:
				if (summary.max() instanceof DenseVector) {
					values = ((DenseVector) summary.max()).getData();
				} else {
					values = ((SparseVector) summary.max()).toDenseVector().getData();
				}
				break;
			case MEAN:
				if (summary.mean() instanceof DenseVector) {
					values = ((DenseVector) summary.mean()).getData();
				} else {
					values = ((SparseVector) summary.mean()).getValues();
				}
				break;
			default:
				meta.set(FILL_VALUE, fillValue);
		}

		List <String> data = new ArrayList <>();
		data.add(JsonConverter.toJson(values));

		return Tuple3.of(meta, data, new ArrayList <>());
	}

	/**
	 * Deserialize the model data.
	 *
	 * @param meta         The model meta data.
	 * @param data         The model concrete data.
	 * @param additionData The additional data.
	 * @return The model data used by mapper.
	 */
	@Override
	public Tuple3 <Strategy, double[], Double> deserializeModel(Params meta, Iterable <String> data,
																Iterable <Row> additionData) {
		Strategy strategy = meta.get(STRATEGY);
		double[] values = null;
		if (data.iterator().hasNext()) {
			values = JsonConverter.fromJson(data.iterator().next(), double[].class);
		}
		Double fillValue = meta.get(FILL_VALUE);
		return Tuple3.of(strategy, values, fillValue);
	}
}
