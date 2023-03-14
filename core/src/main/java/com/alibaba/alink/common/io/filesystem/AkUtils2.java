package com.alibaba.alink.common.io.filesystem;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.stream.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.WhereBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.TableSourceStreamOp;
import com.alibaba.alink.operator.stream.sql.WhereStreamOp;

import java.io.IOException;

public class AkUtils2 {
	public static BatchOperator <?> selectPartitionBatchOp(
		Long mlEnvId, FilePath filePath, String pattern) throws IOException {

		return selectPartitionBatchOp(mlEnvId, filePath, pattern, null);
	}

	public static BatchOperator <?> selectPartitionBatchOp(
		Long mlEnvId, FilePath filePath, String pattern, String[] colNames) throws IOException {

		if (colNames == null) {
			colNames = AkUtils.getPartitionColumns(filePath);
		}
		final int numCols = colNames.length;
		final TypeInformation <?>[] colTypes = new TypeInformation <?>[colNames.length];
		for (int i = 0; i < colNames.length; ++i) {
			colTypes[i] = AlinkTypes.STRING;
		}
		final TableSchema schema = new TableSchema(colNames, colTypes);

		return new TableSourceBatchOp(
			DataSetConversionUtil.toTable(
				mlEnvId,
				MLEnvironmentFactory
					.get(mlEnvId)
					.getExecutionEnvironment()
					.fromElements(0)
					.reduceGroup(new GroupReduceFunction <Integer, Row>() {
						@Override
						public void reduce(Iterable <Integer> values, Collector <Row> out) throws Exception {
							for (Row row : AkUtils.listPartitions(filePath, numCols)) {
								out.collect(row);
							}
						}
					})
					.returns(new RowTypeInfo(schema.getFieldTypes()))
					.rebalance(),
				schema
			))
			.setMLEnvironmentId(mlEnvId)
			.link(
				new WhereBatchOp()
					.setClause(AkUtils.transformPattern(pattern, colNames))
					.setMLEnvironmentId(mlEnvId)
			);
	}

	public static StreamOperator <?> selectPartitionStreamOp(
		Long mlEnvId, FilePath filePath, String pattern) throws IOException {

		return selectPartitionStreamOp(mlEnvId, filePath, pattern, null);
	}

	public static StreamOperator <?> selectPartitionStreamOp(
		Long mlEnvId, FilePath filePath, String pattern, String[] colNames) throws IOException {

		if (colNames == null) {
			colNames = AkUtils.getPartitionColumns(filePath);
		}
		final int numCols = colNames.length;
		final TypeInformation <?>[] colTypes = new TypeInformation <?>[colNames.length];
		for (int i = 0; i < colNames.length; ++i) {
			colTypes[i] = AlinkTypes.STRING;
		}
		final TableSchema schema = new TableSchema(colNames, colTypes);

		return new TableSourceStreamOp(
			DataStreamConversionUtil.toTable(
				mlEnvId,
				MLEnvironmentFactory
					.get(mlEnvId)
					.getStreamExecutionEnvironment()
					.fromElements(0)
					.flatMap(new RichFlatMapFunction <Integer, Row>() {
						@Override
						public void flatMap(Integer value, Collector <Row> out) throws Exception {
							if (value == 0) {
								for (Row row : AkUtils.listPartitions(filePath, numCols)) {
									out.collect(row);
								}
							}
						}
					})
					.returns(new RowTypeInfo(colTypes))
					.rebalance(),
				schema
			))
			.setMLEnvironmentId(mlEnvId)
			.link(
				new WhereStreamOp()
					.setClause(AkUtils.transformPattern(pattern, colNames))
					.setMLEnvironmentId(mlEnvId)
			);
	}
}
