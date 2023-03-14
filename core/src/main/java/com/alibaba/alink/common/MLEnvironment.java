package com.alibaba.alink.common;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable.MTableKryoSerializerV2;
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.lazy.LazyObjectsManager;
import com.alibaba.alink.common.linalg.tensor.Tensor;
import com.alibaba.alink.common.linalg.tensor.TensorKryoSerializer;
import com.alibaba.alink.common.sql.builtin.BuiltInAggRegister;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.stream.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.TableSourceStreamOp;

import java.util.Arrays;
import java.util.List;

/**
 * The MLEnvironment stores the necessary context in Flink.
 * Each MLEnvironment will be associated with a unique ID.
 * The operations associated with the same MLEnvironment ID
 * will share the same Flink job context.
 *
 * <p>Both MLEnvironment ID and MLEnvironment can only be retrieved from MLEnvironmentFactory.
 *
 * @see ExecutionEnvironment
 * @see StreamExecutionEnvironment
 * @see BatchTableEnvironment
 * @see StreamTableEnvironment
 */
public class MLEnvironment {
	private ExecutionEnvironment env;
	private StreamExecutionEnvironment streamEnv;
	private BatchTableEnvironment batchTableEnv;
	private StreamTableEnvironment streamTableEnv;

	final LazyObjectsManager lazyObjectsManager = new LazyObjectsManager();

	/**
	 * Construct with null that the class can load the environment in the `get` method.
	 */
	public MLEnvironment() {
		this(null, null, null, null);
	}

	/**
	 * Construct with the batch environment and the the batch table environment given by user.
	 *
	 * <p>The env can be null which will be loaded in the `get` method.
	 *
	 * @param batchEnv      the ExecutionEnvironment
	 * @param batchTableEnv the BatchTableEnvironment
	 */
	public MLEnvironment(
		ExecutionEnvironment batchEnv,
		BatchTableEnvironment batchTableEnv) {
		this(batchEnv, batchTableEnv, null, null);
	}

	/**
	 * Construct with the stream environment and the the stream table environment given by user.
	 *
	 * <p>The env can be null which will be loaded in the `get` method.
	 *
	 * @param streamEnv      the StreamExecutionEnvironment
	 * @param streamTableEnv the StreamTableEnvironment
	 */
	public MLEnvironment(
		StreamExecutionEnvironment streamEnv,
		StreamTableEnvironment streamTableEnv) {
		this(null, null, streamEnv, streamTableEnv);
	}

	/**
	 * Construct with env given by user.
	 *
	 * <p>The env can be null which will be loaded in the `get` method.
	 *
	 * @param batchEnv       the ExecutionEnvironment
	 * @param batchTableEnv  the BatchTableEnvironment
	 * @param streamEnv      the StreamExecutionEnvironment
	 * @param streamTableEnv the StreamTableEnvironment
	 */
	public MLEnvironment(
		ExecutionEnvironment batchEnv,
		BatchTableEnvironment batchTableEnv,
		StreamExecutionEnvironment streamEnv,
		StreamTableEnvironment streamTableEnv) {
		this.env = batchEnv;
		this.batchTableEnv = batchTableEnv;
		this.streamEnv = streamEnv;
		this.streamTableEnv = streamTableEnv;
		if (this.env != null) {
			env.addDefaultKryoSerializer(MTable.class, new MTableKryoSerializerV2());
			env.addDefaultKryoSerializer(Tensor.class, new TensorKryoSerializer());
		}
		if (this.streamEnv != null) {
			streamEnv.addDefaultKryoSerializer(MTable.class, new MTableKryoSerializerV2());
			streamEnv.addDefaultKryoSerializer(Tensor.class, new TensorKryoSerializer());
		}
		if (this.batchTableEnv != null) {
			BuiltInAggRegister.registerUdf(this.batchTableEnv);
			BuiltInAggRegister.registerUdtf(this.batchTableEnv);
			BuiltInAggRegister.registerUdaf(this.batchTableEnv);
		}
		if (this.streamTableEnv != null) {
			BuiltInAggRegister.registerUdf(this.streamTableEnv);
			BuiltInAggRegister.registerUdtf(this.streamTableEnv);
			BuiltInAggRegister.registerUdaf(this.streamTableEnv);
		}
	}

	/**
	 * Get the ExecutionEnvironment.
	 * if the ExecutionEnvironment has not been set, it initial the ExecutionEnvironment
	 * with default Configuration.
	 *
	 * @return the batch {@link ExecutionEnvironment}
	 */
	public ExecutionEnvironment getExecutionEnvironment() {
		if (null == env) {
			if (ExecutionEnvironment.areExplicitEnvironmentsAllowed()) {
				final int managedMemPerCoreInMB = 64;
				final int networkMemPerCoreInMB = 64;
				final int core = Runtime.getRuntime().availableProcessors();

				Configuration conf = new Configuration();
				conf.setString(
					"taskmanager.memory.managed.size",
					String.format("%dm", managedMemPerCoreInMB * core)
				);
				conf.setString(
					"taskmanager.memory.network.min",
					String.format("%dm", networkMemPerCoreInMB * core)
				);
				env = ExecutionEnvironment.createLocalEnvironment(conf);
				env.setParallelism(core);
			} else {
				env = ExecutionEnvironment.getExecutionEnvironment();
			}

			env.addDefaultKryoSerializer(MTable.class, new MTableKryoSerializerV2());
			env.addDefaultKryoSerializer(Tensor.class, new TensorKryoSerializer());
		}
		return env;
	}

	/**
	 * Get the StreamExecutionEnvironment.
	 * if the StreamExecutionEnvironment has not been set, it initial the StreamExecutionEnvironment
	 * with default Configuration.
	 *
	 * @return the {@link StreamExecutionEnvironment}
	 */
	public StreamExecutionEnvironment getStreamExecutionEnvironment() {
		if (null == streamEnv) {
			streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
			streamEnv.addDefaultKryoSerializer(MTable.class, new MTableKryoSerializerV2());
			streamEnv.addDefaultKryoSerializer(Tensor.class, new TensorKryoSerializer());
		}
		return streamEnv;
	}

	/**
	 * Get the BatchTableEnvironment.
	 * if the BatchTableEnvironment has not been set, it initial the BatchTableEnvironment
	 * with default Configuration.
	 *
	 * @return the {@link BatchTableEnvironment}
	 */
	public BatchTableEnvironment getBatchTableEnvironment() {
		if (null == batchTableEnv) {
			batchTableEnv = BatchTableEnvironment.create(getExecutionEnvironment());
			BuiltInAggRegister.registerUdf(batchTableEnv);
			BuiltInAggRegister.registerUdtf(batchTableEnv);
			BuiltInAggRegister.registerUdaf(batchTableEnv);
		}
		return batchTableEnv;
	}

	/**
	 * Get the StreamTableEnvironment.
	 * if the StreamTableEnvironment has not been set, it initial the StreamTableEnvironment
	 * with default Configuration.
	 *
	 * @return the {@link StreamTableEnvironment}
	 */
	public StreamTableEnvironment getStreamTableEnvironment() {
		if (null == streamTableEnv) {
			streamTableEnv = StreamTableEnvironment.create(getStreamExecutionEnvironment());
			BuiltInAggRegister.registerUdf(streamTableEnv);
			BuiltInAggRegister.registerUdtf(streamTableEnv);
			BuiltInAggRegister.registerUdaf(streamTableEnv);
		}
		return streamTableEnv;
	}

	/**
	 * Evaluates a SQL query on registered tables and retrieves the result as a <code>BatchOperator</code>.
	 *
	 * @param query The SQL query to evaluate.
	 * @return The result of the query as <code>BatchOperator</code>.
	 */
	public BatchOperator <?> batchSQL(String query) {
		return new TableSourceBatchOp(getBatchTableEnvironment().sqlQuery(query));
	}

	/**
	 * Evaluates a SQL query on registered tables and retrieves the result as a <code>StreamOperator</code>.
	 *
	 * @param query The SQL query to evaluate.
	 * @return The result of the query as <code>StreamOperator</code>.
	 */
	public StreamOperator <?> streamSQL(String query) {
		return new TableSourceStreamOp(getStreamTableEnvironment().sqlQuery(query));
	}

	/* open ends here */

	/**
	 * Factory to create batch {@link Table}.
	 *
	 * @param rows     array of rows to create table.
	 * @param colNames the column name of the table.
	 * @return the created batch table.
	 */
	public Table createBatchTable(Row[] rows, String[] colNames) {
		return createBatchTable(Arrays.asList(rows), colNames);
	}

	/**
	 * Factory to create batch {@link Table}.
	 * <p>
	 * We create batch table by session shared ExecutionEnvironment
	 *
	 * @param rows     list of rows to create table.
	 * @param colNames the column name of the table.
	 * @return the created batch table.
	 * @see MLEnvironment#getExecutionEnvironment()
	 * @see MLEnvironment#getBatchTableEnvironment()
	 */
	public Table createBatchTable(List <Row> rows, String[] colNames) {
		if (rows == null || rows.size() < 1) {
			throw new AkIllegalArgumentException("Values can not be empty.");
		}

		Row first = rows.iterator().next();
		int arity = first.getArity();

		TypeInformation <?>[] types = new TypeInformation[arity];

		for (int i = 0; i < arity; ++i) {
			types[i] = TypeExtractor.getForObject(first.getField(i));
		}

		DataSet <Row> dataSet = getExecutionEnvironment().fromCollection(rows);
		return DataSetConversionUtil.toTable(this, dataSet, colNames, types);
	}

	/**
	 * Factory to create stream {@link Table}.
	 *
	 * @param rows     array of rows to create table.
	 * @param colNames the column name of the table.
	 * @return the created stream table.
	 */
	public Table createStreamTable(Row[] rows, String[] colNames) {
		return createStreamTable(Arrays.asList(rows), colNames);
	}

	/**
	 * Factory to create stream {@link Table}.
	 * <p>
	 * We create stream table by session shared StreamExecutionEnvironment
	 *
	 * @param rows     list of rows to create table.
	 * @param colNames the column name of the table.
	 * @return the created stream table.
	 * @see MLEnvironment#getStreamExecutionEnvironment()
	 * @see MLEnvironment#getStreamTableEnvironment()
	 */
	public Table createStreamTable(List <Row> rows, String[] colNames) {
		if (rows == null || rows.size() < 1) {
			throw new AkIllegalArgumentException("Values can not be empty.");
		}

		Row first = rows.iterator().next();
		int arity = first.getArity();

		TypeInformation <?>[] types = new TypeInformation[arity];

		for (int i = 0; i < arity; ++i) {
			types[i] = TypeExtractor.getForObject(first.getField(i));
		}

		DataStream <Row> dataSet = getStreamExecutionEnvironment().fromCollection(rows);
		return DataStreamConversionUtil.toTable(this, dataSet, colNames, types);
	}

	public LazyObjectsManager getLazyObjectsManager() {
		return lazyObjectsManager;
	}
}
