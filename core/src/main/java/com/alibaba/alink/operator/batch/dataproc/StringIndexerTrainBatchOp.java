package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.StringIndexerModelDataConverter;
import com.alibaba.alink.operator.common.dataproc.StringIndexerUtil;
import com.alibaba.alink.params.dataproc.HasStringOrderTypeDefaultAsRandom;
import com.alibaba.alink.params.dataproc.StringIndexerTrainParams;

/**
 * Encode one column of strings to bigint type indices.
 * The indices are consecutive bigint type that start from 0.
 * Non-string columns are first converted to strings and then encoded.
 * <p>
 * <p> Several string order type is supported, including:
 * <ol>
 * <li>random</li>
 * <li>frequency_asc</li>
 * <li>frequency_desc</li>
 * <li>alphabet_asc</li>
 * <li>alphabet_desc</li>
 * </ol>
 */
public final class StringIndexerTrainBatchOp
	extends BatchOperator <StringIndexerTrainBatchOp>
	implements StringIndexerTrainParams <StringIndexerTrainBatchOp> {

	private static final long serialVersionUID = -1198410962987804614L;

	public StringIndexerTrainBatchOp() {
		this(new Params());
	}

	public StringIndexerTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public StringIndexerTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		final String selectedCol = getSelectedCol();
		final HasStringOrderTypeDefaultAsRandom.StringOrderType orderType = getStringOrderType();
		final int selectedColIdx = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), selectedCol);

		DataSet <Row> inputRows = ((DataSet <Row>) in.getDataSet()).map(
			new MapFunction <Row, Row>() {
				private static final long serialVersionUID = 584117860691580161L;

				@Override
				public Row map(Row value) throws Exception {
					return Row.of(value.getField(selectedColIdx));
				}
			}
		);

		DataSet <Tuple3 <Integer, String, Long>> indexedToken =
			StringIndexerUtil.indexTokens(inputRows, orderType, 0L, true);

		DataSet <Row> values = indexedToken
			.mapPartition(new RichMapPartitionFunction <Tuple3 <Integer, String, Long>, Row>() {
				private static final long serialVersionUID = 176349483834372192L;

				@Override
				public void mapPartition(Iterable <Tuple3 <Integer, String, Long>> values, Collector <Row> out)
					throws Exception {
					new StringIndexerModelDataConverter().save(values, out);
				}
			})
			.name("build_model");

		this.setOutput(values, new StringIndexerModelDataConverter().getModelSchema());
		return this;
	}
}

