package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.operator.batch.utils.ExtractModelInfoBatchOp;
import com.alibaba.alink.common.lazy.LazyEvaluation;
import com.alibaba.alink.common.lazy.LazyObjectsManager;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.BaseSourceBatchOp;
import com.alibaba.alink.operator.common.recommendation.AlsModelInfo;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.alink.operator.common.utils.PackBatchOperatorUtil.unpackRows;

/**
 * Als model info batch op.
 */
public class AlsModelInfoBatchOp extends ExtractModelInfoBatchOp <AlsModelInfo, AlsModelInfoBatchOp> {
	private static final long serialVersionUID = -744424427167310133L;
	public final static String USER_NAME = "_alink_user_id";
	public final static String ITEM_NAME = "_alink_item_id";

	public AlsModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected AlsModelInfo createModelInfo(List <Row> rows) {
		int userNum = 0;
		int itemNum = 0;
		int totalSamples = 0;
		for (Row r : rows) {
			if ((long) r.getField(0) == 0L) {
				userNum++;
			} else if ((long) r.getField(0) == 1L) {
				itemNum++;
			} else if ((long) r.getField(0) == 2L) {
				totalSamples++;
			}
		}
		return new AlsModelInfo(userNum, itemNum, totalSamples, getParams());
	}

	@Override
	public AlsModelInfoBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> op = checkAndGetFirst(inputs);
		if (op.isNullOutputTable() && !(op instanceof BaseSourceBatchOp)) {
			LazyObjectsManager lazyObjectsManager = LazyObjectsManager.getLazyObjectsManager(op);
			LazyEvaluation <BatchOperator <?>> lazyOpAfterLinked = lazyObjectsManager.genLazyOpAfterLinked(op);
			lazyOpAfterLinked.addCallback(d -> setOutputTable(d.getOutputTable()));
		} else {
			setOutputTable(op.getOutputTable());
		}

		TypeInformation <?> type = op.getSchema().getFieldTypes()[op.getColNames().length - 1];
		setSideOutputTables(new Table[] {
			getEmbedding(op, 0, op.getMLEnvironmentId(), USER_NAME, type),
			getEmbedding(op, 1, op.getMLEnvironmentId(), ITEM_NAME, type)
		});

		return this;
	}

	public BatchOperator <?> getUserEmbedding() {
		return getSideOutput(0);
	}

	public BatchOperator <?> getItemEmbedding() {
		return getSideOutput(1);
	}

	private Table getEmbedding(BatchOperator <?> model,
							   final Integer idx,
							   long envId,
							   String name,
							   TypeInformation <?> type) {
		DataSet <Row> embedding = model.getDataSet().filter(new FilterFunction <Row>() {
			@Override
			public boolean filter(Row value) {
				return ((long) value.getField(0) == idx.longValue()) || (long) value.getField(0) == -1L;
			}
		}).reduceGroup(new GroupReduceFunction <Row, Row>() {
			@Override
			public void reduce(Iterable <Row> values, Collector <Row> out) {
				List <Row> modelRows = new ArrayList <>();
				for (Row r : values) {
					modelRows.add(r);
				}
				List <Row> embedding = unpackRows(modelRows, idx);
				for (Row e : embedding) {
					out.collect(e);
				}
			}
		});
		return DataSetConversionUtil.toTable(envId, embedding, new String[] {name, "factors"},
			new TypeInformation <?>[] {type, Types.STRING});
	}
}
