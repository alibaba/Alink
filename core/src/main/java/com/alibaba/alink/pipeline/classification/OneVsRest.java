package com.alibaba.alink.pipeline.classification;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.DataSetWrapperBatchOp;
import com.alibaba.alink.operator.batch.sql.UnionAllBatchOp;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.common.io.types.JdbcTypeConverter;
import com.alibaba.alink.params.classification.OneVsRestPredictParams;
import com.alibaba.alink.params.classification.OneVsRestTrainParams;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.linear.HasPositiveLabelValueString;
import com.alibaba.alink.pipeline.EstimatorBase;
import com.alibaba.alink.pipeline.ModelBase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.alibaba.alink.common.utils.JsonConverter.gson;

/**
 * Reduction of Multiclass Classification to Binary Classification.
 * Performs reduction using one against all strategy.
 * For a multiclass classification with k classes, train k models (one per class).
 * Each example is scored against all k models and the model with highest score
 * is picked to label the example.
 */
@NameCn("OneVsRest")
public class OneVsRest extends EstimatorBase <OneVsRest, OneVsRestModel>
	implements OneVsRestTrainParams <OneVsRest>, OneVsRestPredictParams <OneVsRest> {

	private static final long serialVersionUID = -5340633471006011434L;

	private EstimatorBase<?, ?> classifier;

	@Override
	public OneVsRestModel fit(BatchOperator <?> input) {
		String labelColName = classifier.getParams().get(HasLabelCol.LABEL_COL);
		BatchOperator <?> allLabels = getAllLabels(input, labelColName);
		int numClasses = getNumClass();

		int labelColIdx = TableUtil.findColIndexWithAssertAndHint(input.getColNames(), labelColName);
		TypeInformation<?> labelColType = input.getColTypes()[labelColIdx];

		ModelBase <?>[] models = new ModelBase <?>[numClasses];
		for (int iCls = 0; iCls < numClasses; iCls++) {
			this.classifier.set(HasPositiveLabelValueString.POS_LABEL_VAL_STR, "1");

			BatchOperator <?> trainData = generateTrainData(input, allLabels, iCls, labelColIdx);

			models[iCls] = this.classifier.fit(trainData);
		}

		Table modelData = unionAllModels(models);

		Params meta = new Params()
			.set(ModelParamName.NUM_CLASSES, numClasses)
			.set(ModelParamName.BIN_CLS_CLASS_NAME, this.classifier.getClass().getCanonicalName())
			.set(ModelParamName.BIN_CLS_PARAMS, this.classifier.getParams().toJson())
			.set(ModelParamName.LABEL_TYPE_NAME, FlinkTypeConverter.getTypeString(labelColType))
			.set(ModelParamName.MODEL_COL_NAMES, models[0].getModelData().getSchema().getFieldNames())
			.set(ModelParamName.MODEL_COL_TYPES,
				toJdbcColTypes(models[0].getModelData().getSchema().getFieldTypes()));

		Table modelMeta = createModelMeta(meta, allLabels);

		OneVsRestModel oneVsRestModel = new OneVsRestModel(classifier.getParams().clone().merge(this.getParams()));
		oneVsRestModel.setModelData(BatchOperator.fromTable(
			concatTables(new Table[] {modelMeta, modelData, allLabels.getOutputTable()},
				getMLEnvironmentId())
		));
		return oneVsRestModel;
	}

	public OneVsRest setClassifier(EstimatorBase<?, ?> classifier) {
		this.classifier = classifier;
		return this;
	}

	private static Table concatTables(Table[] tables, Long sessionId) {
		final int[] numCols = new int[tables.length];
		final List <String> allColNames = new ArrayList <>();
		final List <TypeInformation <?>> allColTypes = new ArrayList <>();
		allColNames.add("table_id");
		allColTypes.add(Types.LONG);
		for (int i = 0; i < tables.length; i++) {
			if (tables[i] == null) {
				numCols[i] = 0;
			} else {
				numCols[i] = tables[i].getSchema().getFieldNames().length;
				String[] prefixedColNames = tables[i].getSchema().getFieldNames().clone();
				for (int j = 0; j < prefixedColNames.length; j++) {
					prefixedColNames[j] = String.format("t%d_%s", i, prefixedColNames[j]);
				}
				allColNames.addAll(Arrays.asList(prefixedColNames));
				allColTypes.addAll(Arrays.asList(tables[i].getSchema().getFieldTypes()));
			}
		}

		if (allColNames.size() == 1) {
			return null;
		}

		DataSet <Row> allRows = null;
		int startCol = 1;
		final int numAllCols = allColNames.size();
		for (int i = 0; i < tables.length; i++) {
			if (tables[i] == null) {
				continue;
			}
			final int constStartCol = startCol;
			final int iTable = i;
			DataSet <Row> rows = BatchOperator.fromTable(tables[i]).setMLEnvironmentId(sessionId).getDataSet();
			rows = rows.map(new RichMapFunction <Row, Row>() {
				private static final long serialVersionUID = -8085823678072944808L;
				transient Row reused;

				@Override
				public void open(Configuration parameters) {
					reused = new Row(numAllCols);
				}

				@Override
				public Row map(Row value) {
					for (int i = 0; i < numAllCols; i++) {
						reused.setField(i, null);
					}
					reused.setField(0, (long) iTable);
					for (int i = 0; i < numCols[iTable]; i++) {
						reused.setField(constStartCol + i, value.getField(i));
					}
					return reused;
				}
			});
			if (allRows == null) {
				allRows = rows;
			} else {
				allRows = allRows.union(rows);
			}
			startCol += numCols[i];
		}
		return DataSetConversionUtil.toTable(sessionId, allRows, allColNames.toArray(new String[0]),
			allColTypes.toArray(new TypeInformation[0]));
	}

	private static BatchOperator <?> generateTrainData(
		BatchOperator <?> data, BatchOperator <?> allLabels,
		final int iLabel, final int labelColIdx) {

		DataSet <Row> dataSet = data.getDataSet();
		dataSet = dataSet
			.map(new RichMapFunction <Row, Row>() {
				private static final long serialVersionUID = 6739243159349750842L;
				transient Object label;

				@Override
				public void open(Configuration parameters) throws Exception {
					List <Row> bc = getRuntimeContext().getBroadcastVariable("allLabels");
					Integer[] order = new Integer[bc.size()];
					for (int i = 0; i < order.length; i++) {
						order[i] = i;
					}
					Arrays.sort(order, new Comparator <Integer>() {
						@Override
						public int compare(Integer o1, Integer o2) {
							Comparable v1 = (Comparable) bc.get(o1).getField(0);
							Comparable v2 = (Comparable) bc.get(o2).getField(0);
							return v1.compareTo(v2);
						}
					});
					if (iLabel >= bc.size()) {
						throw new RuntimeException(
							"the specified numClasses is larger than the number of distinct labels.: " +
								String.format("iLabel = %d, num lables = %d", iLabel, bc.size()));
					}
					this.label = bc.get(order[iLabel]).getField(0);
				}

				@Override
				public Row map(Row value) {
					for (int i = 0; i < value.getArity(); i++) {
						if (i == labelColIdx) {
							if (value.getField(i).equals(label)) {
								value.setField(i, 1.0);
							} else {
								value.setField(i, 0.0);
							}
						}
					}
					return value;
				}
			})
			.withBroadcastSet(allLabels.getDataSet(), "allLabels")
			.name("CreateTrainData#" + iLabel);

		TypeInformation<?>[] colTypes = data.getColTypes().clone();
		colTypes[labelColIdx] = Types.DOUBLE;
		return new DataSetWrapperBatchOp(dataSet, data.getColNames(), colTypes)
			.setMLEnvironmentId(data.getMLEnvironmentId());
	}

	private Table createModelMeta(final Params meta, BatchOperator <?> allLabels) {
		DataSet <Row> metaDataSet = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment()
			.fromElements(meta.toJson())
			.map(new RichMapFunction <String, String>() {
				private static final long serialVersionUID = 6749489554360703883L;

				@Override
				public String map(String value) throws Exception {
					Params params = Params.fromJson(value);
					List <Row> bc = getRuntimeContext().getBroadcastVariable("allLabels");
					Integer[] order = new Integer[bc.size()];
					for (int i = 0; i < order.length; i++) {
						order[i] = i;
					}
					Arrays.sort(order, (o1, o2) -> {
						Comparable v1 = (Comparable) bc.get(o1).getField(0);
						Comparable v2 = (Comparable) bc.get(o2).getField(0);
						return v1.compareTo(v2);
					});
					List <Object> orderedLabels = new ArrayList <>(order.length);
					for (Integer integer : order) {
						orderedLabels.add(bc.get(integer).getField(0));
					}
					params.set(ModelParamName.LABELS, gson.toJson(orderedLabels, ArrayList.class));
					return params.toJson();
				}
			})
			.withBroadcastSet(allLabels.getDataSet(), "allLabels")
			.map(new MapFunction <String, Row>() {
				private static final long serialVersionUID = 7457969876114448730L;

				@Override
				public Row map(String value) throws Exception {
					return Row.of(value);
				}
			});
		return DataSetConversionUtil.toTable(getMLEnvironmentId(), metaDataSet, new String[] {"meta"},
			new TypeInformation[] {Types.STRING});
	}

	private Table unionAllModels(ModelBase<?>[] models) {
		BatchOperator <?> allModel = null;
		for (int i = 0; i < models.length; i++) {
			BatchOperator <?> model = models[i].getModelData();
			model = model.select(String.format("CAST(%d as bigint) AS ovr_id, *", i));
			if (i == 0) {
				allModel = model;
			} else {
				allModel = new UnionAllBatchOp()
					.setMLEnvironmentId(getMLEnvironmentId())
					.linkFrom(allModel, model);
			}
		}
		return allModel.getOutputTable();
	}

	private Integer[] toJdbcColTypes(TypeInformation<?>[] colTypes) {
		Integer[] typesInInt = new Integer[colTypes.length];
		for (int i = 0; i < colTypes.length; i++) {
			typesInInt[i] = JdbcTypeConverter.getIntegerSqlType(colTypes[i]);
		}
		return typesInInt;
	}

	private BatchOperator <?> getAllLabels(BatchOperator <?> data, String labelColName) {
		final int labelColIdx = TableUtil.findColIndexWithAssertAndHint(data.getColNames(), labelColName);
		DataSet <Row> input = data.getDataSet();
		DataSet <Row> output = input
			.mapPartition(new MapPartitionFunction <Row, Tuple1 <Comparable>>() {
				private static final long serialVersionUID = 8885186467183165174L;

				@Override
				public void mapPartition(Iterable <Row> values, Collector <Tuple1 <Comparable>> out) {
					Set <Object> distinctLabels = new HashSet <>();
					values.forEach(row -> {
						distinctLabels.add(row.getField(labelColIdx));
					});
					distinctLabels.forEach(l -> {
						out.collect(Tuple1.of((Comparable) l));
					});
				}
			})
			.groupBy(0)
			.reduceGroup(new GroupReduceFunction <Tuple1 <Comparable>, Row>() {
				private static final long serialVersionUID = -3338967361764454088L;

				@Override
				public void reduce(Iterable <Tuple1 <Comparable>> values, Collector <Row> out) throws Exception {
					out.collect(Row.of(values.iterator().next().f0));
				}
			});
		return new DataSetWrapperBatchOp(output, new String[] {labelColName},
			new TypeInformation[] {data.getColTypes()[labelColIdx]})
			.setMLEnvironmentId(data.getMLEnvironmentId());
	}
}
