package com.alibaba.alink.operator.common.tree;

import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class TreeModelInfoBatchOp<S, T extends TreeModelInfoBatchOp <S, T>>
	extends ExtractModelInfoBatchOp <S, T> {

	private static final long serialVersionUID = 1735133462550836751L;

	public TreeModelInfoBatchOp() {
		this(null);
	}

	public TreeModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> processModel() {
		return combinedTreeModelFeatureImportance(
			this,
			new TableSourceBatchOp(
				DataSetConversionUtil
					.toTable(
						getMLEnvironmentId(),
						this.getDataSet().reduceGroup(
							new TreeModelDataConverter.FeatureImportanceReducer()
						),
						new String[] {
							getParams().get(TreeModelDataConverter.IMPORTANCE_FIRST_COL),
							getParams().get(TreeModelDataConverter.IMPORTANCE_SECOND_COL)
						},
						new TypeInformation[] {Types.STRING, Types.DOUBLE}
					)
			).setMLEnvironmentId(getMLEnvironmentId())
		);
	}

	private static BatchOperator <?> combinedTreeModelFeatureImportance(
		BatchOperator <?> model, BatchOperator <?> featureImportance) {

		DataSet <String> importanceJson =
			featureImportance
				.getDataSet()
				.reduceGroup(new GroupReduceFunction <Row, String>() {
					private static final long serialVersionUID = -1576541700351312745L;

					@Override
					public void reduce(Iterable <Row> values, Collector <String> out) throws Exception {
						Map <String, Double> importance = new HashMap <>();

						for (Row val : values) {
							importance.put(
								String.valueOf(val.getField(0)),
								((Number) val.getField(1)).doubleValue()
							);
						}

						out.collect(JsonConverter.toJson(importance));
					}
				});

		DataSet <Row> combined =
			model.getDataSet()
				.reduceGroup(new RichGroupReduceFunction <Row, Row>() {
					private static final long serialVersionUID = -1576541700351312745L;
					private transient String featureImportanceJson;

					@Override
					public void open(Configuration parameters) throws Exception {
						super.open(parameters);

						featureImportanceJson = getRuntimeContext()
							.getBroadcastVariableWithInitializer(
								"importanceJson",
								new BroadcastVariableInitializer <String, String>() {
									@Override
									public String initializeBroadcastVariable(Iterable <String> data) {
										return data.iterator().next();
									}
								}
							);
					}

					@Override
					public void reduce(Iterable <Row> values, Collector <Row> out) throws Exception {
						List <Row> modelRows = new ArrayList <>();

						for (Row val : values) {
							modelRows.add(val);
						}

						TreeModelDataConverter model
							= new TreeModelDataConverter().load(modelRows);

						model.meta.set(
							TreeModelInfo.FEATURE_IMPORTANCE,
							featureImportanceJson
						);

						model.save(model, out);
					}
				})
				.withBroadcastSet(importanceJson, "importanceJson");

		return new TableSourceBatchOp(
			DataSetConversionUtil.toTable(
				model.getMLEnvironmentId(),
				combined,
				model.getColNames(),
				model.getColTypes()
			)
		);
	}
}
