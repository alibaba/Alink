package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.SelectedColsWithFirstInputSpec;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.HugeStringIndexerUtil;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelDataConverter;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.params.dataproc.HasSelectedColTypes;
import com.alibaba.alink.params.dataproc.HasStringOrderTypeDefaultAsRandom;
import com.alibaba.alink.params.dataproc.MultiStringIndexerTrainParams;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

/**
 * Encode several columns of strings to bigint type indices. The indices are consecutive bigint type
 * that start from 0. Non-string columns are first converted to strings and then encoded. Each columns
 * are encoded separately.
 * <p>
 * <p>Several string order type is supported, including:
 * <ol>
 * <li>random</li>
 * <li>frequency_asc</li>
 * <li>frequency_desc</li>
 * <li>alphabet_asc</li>
 * <li>alphabet_desc</li>
 * </ol>
 */
@InputPorts(values = @PortSpec(value = PortType.DATA, opType = OpType.BATCH))
@OutputPorts(values = @PortSpec(value = PortType.MODEL))
@SelectedColsWithFirstInputSpec
@NameCn("MultiStringIndexer训练")
public final class MultiStringIndexerTrainBatchOp
	extends BatchOperator <MultiStringIndexerTrainBatchOp>
	implements MultiStringIndexerTrainParams <MultiStringIndexerTrainBatchOp> {

	private static final long serialVersionUID = 3760905390429627737L;

	public MultiStringIndexerTrainBatchOp() {
		this(new Params());
	}

	public MultiStringIndexerTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public MultiStringIndexerTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		final String[] selectedColNames = getSelectedCols();
		final HasStringOrderTypeDefaultAsRandom.StringOrderType orderType = getStringOrderType();

		final String[] selectedColSqlType = new String[selectedColNames.length];
		for (int i = 0; i < selectedColNames.length; i++) {
			selectedColSqlType[i] = FlinkTypeConverter.getTypeString(
				TableUtil.findColTypeWithAssertAndHint(in.getSchema(), selectedColNames[i]));
		}

		DataSet <Tuple2 <Integer, String>> inputRows = in.select(selectedColNames).getDataSet()
			.flatMap(new FlatMapFunction <Row, Tuple2 <Integer, String>>() {
				@Override
				public void flatMap(Row row, Collector <Tuple2 <Integer, String>> collector) throws Exception {
					for (int i = 0; i < selectedColNames.length; i++) {
						Object o = row.getField(i);
						if (null != o) {
							collector.collect(Tuple2.of(i, String.valueOf(o)));
						}
					}
				}
			})
			.returns(new TupleTypeInfo <>(Types.INT, Types.STRING));

		DataSet <Tuple3 <Integer, String, Long>> indexedToken =
			HugeStringIndexerUtil.indexTokens(inputRows, orderType, 0L);

		DataSet <Row> values = indexedToken
			.mapPartition(new RichMapPartitionFunction <Tuple3 <Integer, String, Long>, Row>() {
				private static final long serialVersionUID = 2876851020570715540L;

				@Override
				public void mapPartition(Iterable <Tuple3 <Integer, String, Long>> values, Collector <Row> out)
					throws Exception {
					Params meta = null;
					if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
						meta = new Params().set(HasSelectedCols.SELECTED_COLS, selectedColNames)
							.set(HasSelectedColTypes.SELECTED_COL_TYPES, selectedColSqlType);
					}
					new MultiStringIndexerModelDataConverter().save(Tuple2.of(meta, values), out);
				}
			})
			.name("build_model")
			.returns(new RowTypeInfo(new MultiStringIndexerModelDataConverter().getModelSchema().getFieldTypes()));

		this.setOutput(values, new MultiStringIndexerModelDataConverter().getModelSchema());
		return this;
	}
}

