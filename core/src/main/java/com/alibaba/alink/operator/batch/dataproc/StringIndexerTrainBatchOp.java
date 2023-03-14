package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.HugeStringIndexerUtil;
import com.alibaba.alink.operator.common.dataproc.StringIndexerModelDataConverter;
import com.alibaba.alink.params.dataproc.HasStringOrderTypeDefaultAsRandom;
import com.alibaba.alink.params.dataproc.StringIndexerTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

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
@InputPorts(values = @PortSpec(value = PortType.DATA))
@OutputPorts(values = @PortSpec(value = PortType.MODEL))
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.INT_LONG_STRING_TYPES)
@ParamSelectColumnSpec(name = "selectedCols", allowedTypeCollections = TypeCollections.INT_LONG_STRING_TYPES)
@NameCn("字符串编码训练")
@NameEn("String Indexer Train")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.dataproc.StringIndexer")
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
		final String[] selectedCols = getSelectedCols();
		String[] allSelectCols = new String[]{selectedCol};
		if (selectedCols != null) {
			allSelectCols = new String[selectedCols.length + 1];
			allSelectCols[0] = selectedCol;
			for (int i = 0; i < selectedCols.length; i++) {
				allSelectCols[i + 1] = selectedCols[i];
			}
		}
		final HasStringOrderTypeDefaultAsRandom.StringOrderType orderType = getStringOrderType();
		TypeInformation[] types = TableUtil.findColTypes(in.getSchema(), allSelectCols);
		for (TypeInformation type : types) {
			if (!type.equals(types[0])) {
				throw new AkIllegalOperatorParameterException("All selectCols must be the same type!");
			}
		}
		DataSet <Tuple2 <Integer, String>> inputRows = ((DataSet <Row>) in.select(allSelectCols).getDataSet()).flatMap(
			new FlatMapFunction <Row, Tuple2 <Integer, String>>() {
				@Override
				public void flatMap(Row row, Collector <Tuple2 <Integer, String>> collector) throws Exception {
					for (int i = 0; i < row.getArity(); i++) {
						if (row.getField(i) != null) {
							collector.collect(Tuple2.of(0, String.valueOf(row.getField(i))));
						}
					}
				}

				private static final long serialVersionUID = 584117860691580161L;
			}
		)
			.name("flatten_input_feature")
			.returns(new TupleTypeInfo <>(Types.INT, Types.STRING));

		DataSet <Tuple3 <Integer, String, Long>> indexedToken =
			HugeStringIndexerUtil.indexTokens(inputRows, orderType, 0L);

		DataSet <Row> values = indexedToken
			.mapPartition(new RichMapPartitionFunction <Tuple3 <Integer, String, Long>, Row>() {
				private static final long serialVersionUID = 176349483834372192L;

				@Override
				public void mapPartition(Iterable <Tuple3 <Integer, String, Long>> values, Collector <Row> out)
					throws Exception {
					new StringIndexerModelDataConverter().save(values, out);
				}
			})
			.name("build_model")
			.returns(new RowTypeInfo(new StringIndexerModelDataConverter().getModelSchema().getFieldTypes()));

		this.setOutput(values, new StringIndexerModelDataConverter().getModelSchema());
		return this;
	}
}

