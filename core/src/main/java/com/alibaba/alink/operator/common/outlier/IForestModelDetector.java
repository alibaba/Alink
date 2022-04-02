package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.model.RichModelDataConverter;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.dataproc.NumericalTypeCastMapper;
import com.alibaba.alink.operator.common.outlier.IForestDetector.IForestPredict;
import com.alibaba.alink.operator.common.outlier.IForestDetector.Node;
import com.alibaba.alink.params.dataproc.HasTargetType.TargetType;
import com.alibaba.alink.params.dataproc.NumericalTypeCastParams;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class IForestModelDetector extends ModelOutlierDetector {

	private static final double DEFAULT_THRESHOLD = 0.5;

	private transient IForestPredict iForestPredictor;

	private transient NumericalTypeCastMapper numericalTypeCastMapper;

	private transient Function <Row, Row> expandToRow;

	public IForestModelDetector(TableSchema modelSchema,
								TableSchema dataSchema,
								Params params) {
		super(modelSchema, dataSchema, params);
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		IForestModel model = new IForestModelDataConverter().load(modelRows);

		iForestPredictor = new IForestPredict();
		iForestPredictor.loadModel(new IForestModelDataConverter().load(modelRows));

		if (model.meta.contains(WithMultiVarParams.VECTOR_COL)) {
			final int vectorIndex = TableUtil.findColIndexWithAssertAndHint(
				getDataSchema(), model.meta.get(WithMultiVarParams.VECTOR_COL)
			);
			final int maxVectorSize = model.meta.get(OutlierUtil.MAX_VECTOR_SIZE);

			numericalTypeCastMapper = null;

			expandToRow = new Function <Row, Row>() {
				@Override
				public Row apply(Row row) {
					return OutlierUtil.vectorToRow((Vector) row.getField(vectorIndex), maxVectorSize);
				}
			};
		} else {
			final int[] indices = TableUtil.findColIndicesWithAssertAndHint(
				getDataSchema(),
				model.meta.get(WithMultiVarParams.FEATURE_COLS)
			);
			numericalTypeCastMapper = new NumericalTypeCastMapper(
				getDataSchema(),
				new Params()
					.set(NumericalTypeCastParams.SELECTED_COLS, model.meta.get(WithMultiVarParams.FEATURE_COLS))
					.set(NumericalTypeCastParams.TARGET_TYPE, TargetType.DOUBLE)
			);
			expandToRow = new Function <Row, Row>() {
				@Override
				public Row apply(Row row) {
					return Row.project(row, indices);
				}
			};
		}
	}

	@Override
	protected Tuple3 <Boolean, Double, Map <String, String>> detect(SlicedSelectedSample selection) throws Exception {
		Row input = new Row(selection.length());

		for (int i = 0; i < selection.length(); ++i) {
			input.setField(i, selection.get(i));
		}

		if (numericalTypeCastMapper != null) {
			input = numericalTypeCastMapper.map(input);
		}

		input = expandToRow.apply(input);

		double score = iForestPredictor.predict(input);

		return Tuple3.of(score > DEFAULT_THRESHOLD, score, null);
	}

	public static class IForestModel implements Serializable {
		public Params meta = new Params();
		public List <List <IForestDetector.Node>> trees = new ArrayList <>();
	}

	public static class IForestModelDataConverter
		extends RichModelDataConverter <IForestModel, IForestModel> {

		@Override
		public Tuple3 <Params, Iterable <String>, Iterable <Row>> serializeModel(IForestModel modelData) {
			return Tuple3.of(
				modelData.meta, () -> new Iterator <String>() {
					int cursor = 0;

					@Override
					public boolean hasNext() {
						return cursor < modelData.trees.size();
					}

					@Override
					public String next() {
						return JsonConverter.toJson(modelData.trees.get(cursor++));
					}
				}, null
			);
		}

		@Override
		public IForestModel deserializeModel(
			Params meta, Iterable <String> data, Iterable <Row> additionData) {

			IForestModel model = new IForestModel();

			model.meta = meta;
			for (String str : data) {
				model.trees.add(
					JsonConverter.fromJson(str, new TypeReference <List <Node>>() {}.getType())
				);
			}

			return model;
		}

		@Override
		protected String[] initAdditionalColNames() {
			return new String[0];
		}

		@Override
		protected TypeInformation <?>[] initAdditionalColTypes() {
			return new TypeInformation <?>[0];
		}

	}
}
