package com.alibaba.alink.operator.batch;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.MLEnvironment;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.exceptions.AkFlinkExecutionErrorException;
import com.alibaba.alink.common.exceptions.AkIllegalOperationException;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.lazy.LazyEvaluation;
import com.alibaba.alink.common.lazy.LazyObjectsManager;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.batch.dataproc.FirstNBatchOp;
import com.alibaba.alink.operator.batch.dataproc.RebalanceBatchOp;
import com.alibaba.alink.operator.batch.dataproc.SampleBatchOp;
import com.alibaba.alink.operator.batch.dataproc.SampleWithSizeBatchOp;
import com.alibaba.alink.operator.batch.dataproc.ShuffleBatchOp;
import com.alibaba.alink.operator.batch.sink.BaseSinkBatchOp;
import com.alibaba.alink.operator.batch.source.BaseSourceBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.GroupByBatchOp;
import com.alibaba.alink.operator.batch.sql.SelectBatchOp;
import com.alibaba.alink.operator.batch.statistics.InternalFullStatsBatchOp;
import com.alibaba.alink.operator.batch.statistics.SummarizerBatchOp;
import com.alibaba.alink.operator.batch.utils.DiveVisualizer.DiveVisualizerConsumer;
import com.alibaba.alink.operator.batch.utils.UDFBatchOp;
import com.alibaba.alink.operator.batch.utils.UDTFBatchOp;
import com.alibaba.alink.operator.common.sql.BatchSqlOperators;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Base class of batch algorithm operators.
 * <p>
 * <p>This class extends {@link AlgoOperator} to support data transmission between BatchOperators.
 */
public abstract class BatchOperator<T extends BatchOperator <T>> extends AlgoOperator <T> {

	private static final long serialVersionUID = -6336939062258340584L;

	public BatchOperator() {
		this(null);
	}

	/**
	 * The constructor of BatchOperator with {@link Params}.
	 *
	 * @param params the initial Params.
	 */
	public BatchOperator(Params params) {
		super(params);
	}

	/**
	 * Link to another {@link BatchOperator}.
	 * <p>
	 * <p>Link the <code>next</code> BatchOperator using this BatchOperator as its input.
	 * <p>
	 * <p>For example:
	 * <p>
	 * <pre>
	 * {@code
	 * BatchOperator a = ...;
	 * BatchOperator b = ...;
	 * BatchOperator c = a.link(b)
	 * }
	 * </pre>
	 * <p>
	 * <p>The BatchOperator <code>c</code> in the above code
	 * is the same instance as <code>b</code> which takes
	 * <code>a</code> as its input.
	 * Note that BatchOperator <code>b</code> will be changed
	 * to link from BatchOperator <code>a</code>.
	 *
	 * @param next The operator that will be modified to add this operator to its input.
	 * @param <B>  type of BatchOperator returned
	 * @return the linked next
	 * @see #linkFrom(BatchOperator[])
	 */
	public <B extends BatchOperator <?>> B link(B next) {
		next.linkFrom(this);
		return next;
	}

	/**
	 * Link from others {@link BatchOperator}.
	 * <p>
	 * <p>Link this object to BatchOperator using the BatchOperators as its input.
	 * <p>
	 * <p>For example:
	 * <p>
	 * <pre>
	 * {@code
	 * BatchOperator a = ...;
	 * BatchOperator b = ...;
	 * BatchOperator c = ...;
	 *
	 * BatchOperator d = c.linkFrom(a, b)
	 * }
	 * </pre>
	 * <p>
	 * <p>The <code>d</code> in the above code is the same
	 * instance as BatchOperator <code>c</code> which takes
	 * both <code>a</code> and <code>b</code> as its input.
	 * <p>
	 * <p>note: It is not recommended to linkFrom itself or linkFrom the same group inputs twice.
	 *
	 * @param inputs the linked inputs
	 * @return the linked this object
	 */
	public abstract T linkFrom(BatchOperator <?>... inputs);

	/**
	 * create a new BatchOperator from table.
	 *
	 * @param table the input table
	 * @return the new BatchOperator
	 */
	public static BatchOperator <?> fromTable(Table table) {
		return new TableSourceBatchOp(table);
	}

	protected static BatchOperator <?> checkAndGetFirst(BatchOperator <?>... inputs) {
		checkOpSize(1, inputs);
		return inputs[0];
	}

	/**
	 * Get the {@link DataSet} that casted from the output table with the type of {@link Row}.
	 *
	 * @return the casted {@link DataSet}
	 */
	public DataSet <Row> getDataSet() {
		return DataSetConversionUtil.fromTable(getMLEnvironmentId(), getOutputTable());
	}

	@Override
	public BatchOperator <?> select(String fields) {
		return new SelectBatchOp(fields).setMLEnvironmentId(this.getMLEnvironmentId()).linkFrom(this);
	}

	@Override
	public BatchOperator <?> select(String[] fields) {
		return select(TableUtil.columnsToSqlClause(fields));
	}

	@Override
	public BatchOperator <?> as(String fields) {
		return BatchSqlOperators.as(this, fields);
	}

	@Override
	public BatchOperator <?> as(String[] fields) {
		StringBuilder sbd = new StringBuilder();
		for (int i = 0; i < fields.length; i++) {
			if (i > 0) {
				sbd.append(",");
			}
			sbd.append(fields[i]);
		}
		return as(sbd.toString());
	}

	@Override
	public BatchOperator <?> where(String predicate) {
		return BatchSqlOperators.where(this, predicate);
	}

	@Override
	public BatchOperator <?> filter(String predicate) {
		return BatchSqlOperators.filter(this, predicate);
	}

	/**
	 * Remove duplicated records.
	 *
	 * @return The resulted <code>BatchOperator</code> of the "distinct" operation.
	 */
	public BatchOperator <?> distinct() {
		return BatchSqlOperators.distinct(this);
	}

	public BatchOperator <?> orderBy(String fieldName, int limit, boolean isAscending) {
		return BatchSqlOperators.orderBy(this, fieldName, isAscending, limit);
	}

	/**
	 * Order the records by a specific field and keeping a specific range of records.
	 *
	 * @param fieldName The name of the field by which the records are ordered.
	 * @param offset    The starting position of records to keep.
	 * @param fetch     The  number of records to keep.
	 * @return The resulted <code>BatchOperator</code> of the "orderBy" operation.
	 */
	public BatchOperator <?> orderBy(String fieldName, int offset, int fetch) {
		return orderBy(fieldName, offset, fetch, true);
	}

	public BatchOperator <?> orderBy(String fieldName, int offset, int fetch, boolean isAscending) {
		return BatchSqlOperators.orderBy(this, fieldName, isAscending, offset, fetch);
	}

	/**
	 * Order the records by a specific field and keeping a limited number of records.
	 *
	 * @param fieldName The name of the field by which the records are ordered.
	 * @param limit     The maximum number of records to keep.
	 * @return The resulted <code>BatchOperator</code> of the "orderBy" operation.
	 */
	public BatchOperator <?> orderBy(String fieldName, int limit) {
		return orderBy(fieldName, limit, true);
	}

	/**
	 * Apply the "group by" operation.
	 *
	 * @param groupByPredicate The predicate specifying the fields from which records are grouped.
	 * @param selectClause     The clause specifying the fields to select and the aggregation operations.
	 * @return The resulted <code>BatchOperator</code> of the "groupBy" operation.
	 */
	public BatchOperator <?> groupBy(String groupByPredicate, String selectClause) {
		return new GroupByBatchOp(groupByPredicate, selectClause).setMLEnvironmentId(this.getMLEnvironmentId())
			.linkFrom(this);
	}

	public BatchOperator <?> rebalance() {
		return new RebalanceBatchOp().linkFrom(this);
	}

	public BatchOperator <?> shuffle() {
		return new ShuffleBatchOp().linkFrom(this);
	}

	/* open ends here */

	/**
	 * Alias of {@link #link(BatchOperator)}}
	 */
	public <B extends BatchOperator <?>> B linkTo(B next) {
		return link(next);
	}

	public T linkFrom(List <BatchOperator <?>> ins) {
		return linkFrom(ins.toArray(new BatchOperator <?>[0]));
	}

	public static void execute() throws Exception {
		triggerLazyEvaluation(MLEnvironmentFactory.getDefault());
	}

	public static void execute(String jobName) throws Exception {
		triggerLazyEvaluation(MLEnvironmentFactory.getDefault(), jobName);
	}

	public static void execute(MLEnvironment mlEnv) throws Exception {
		triggerLazyEvaluation(mlEnv);
	}

	public static void execute(MLEnvironment mlEnv, String jobName) throws Exception {
		triggerLazyEvaluation(mlEnv, jobName);
	}

	public static void setParallelism(int parallelism) {
		MLEnvironmentFactory.getDefault().getExecutionEnvironment().setParallelism(parallelism);
	}

	@Deprecated
	public static void disableLogging() {
		// pass
	}

	public long count() throws Exception {
		return DataSetConversionUtil.fromTable(getMLEnvironmentId(), getOutputTable()).count();
	}

	protected void setOutput(DataSet <Row> dataSet, TableSchema schema) {
		setOutputTable(DataSetConversionUtil.toTable(getMLEnvironmentId(), dataSet, schema));
	}

	protected void setOutput(DataSet <Row> dataSet, String[] colNames) {
		setOutputTable(DataSetConversionUtil.toTable(getMLEnvironmentId(), dataSet, colNames));
	}

	protected void setOutput(DataSet <Row> dataSet, String[] colNames, TypeInformation <?>[] colTypes) {
		setOutputTable(DataSetConversionUtil.toTable(getMLEnvironmentId(), dataSet, colNames, colTypes));
	}

	public List <Row> collect() {
		MLEnvironment mlEnv = MLEnvironmentFactory.get(getMLEnvironmentId());
		LazyEvaluation <Pair <BatchOperator <?>, List <Row>>> lazyRows = mlEnv.getLazyObjectsManager().genLazySink(
			this);
		triggerLazyEvaluation(mlEnv);
		return lazyRows.getLatestValue().getRight();
	}

	public MTable collectMTable() {
		return new MTable(collect(), getSchema());
	}

	@Deprecated
	public String getTableName() {
		Table outputTable = getOutputTable();
		Preconditions.checkNotNull(outputTable, "This output table is null.");
		return outputTable.toString();
	}

	/**
	 * Register the table of this operator to its table environment.
	 * An operator can register multiple times with different names.
	 *
	 * @param name The name to register with.
	 * @return This operator.
	 */
	public BatchOperator <?> registerTableName(String name) {
		MLEnvironmentFactory.get(getMLEnvironmentId()).getBatchTableEnvironment().registerTable(name, getOutputTable
			());
		return this;
	}

	public static void registerFunction(String name, ScalarFunction function) {
		MLEnvironmentFactory.getDefault().getBatchTableEnvironment().registerFunction(name, function);
	}

	public static <T> void registerFunction(String name, TableFunction <T> function) {
		MLEnvironmentFactory.getDefault().getBatchTableEnvironment().registerFunction(name, function);
	}

	/**
	 * Evaluate SQL query within the default {@link MLEnvironment}.
	 *
	 * @param query The query to evaluate.
	 * @return The evaluation result returned as a {@link BatchOperator}.
	 */
	public static BatchOperator <?> sqlQuery(String query) {
		final MLEnvironment env = MLEnvironmentFactory.getDefault();
		final Long sessionId = MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID;
		return env.batchSQL(query).setMLEnvironmentId(sessionId);
	}

	public BatchOperator <?> getSideOutput(int idx) {
		if (null == this.getSideOutputTables()) {
			throw new AkIllegalOperationException("There is no side output. "
				+ "Please call 'link' method firstly, or this BatchOperator has no SideOutput.");
		} else if (idx < 0 || idx >= this.getSideOutputTables().length) {
			throw new AkIllegalOperationException(
				"The index of side output, #" + idx + " , is out of range. Total number of side outputs is "
					+ this.getSideOutputCount() + ".");
		} else {
			return new TableSourceBatchOp(this.getSideOutputTables()[idx]).setMLEnvironmentId(getMLEnvironmentId());
		}
	}

	public int getSideOutputCount() {
		return null == this.getSideOutputTables() ? 0 : this.getSideOutputTables().length;
	}

	@Override
	public T print() throws Exception {
		return print(0);
	}

	public T print(String title) throws Exception {
		return print(0, title);
	}

	public T print(int n) throws Exception {
		return print(n, null);
	}

	public T print(int n, String title) throws Exception {
		this.lazyPrint(n, title);
		triggerLazyEvaluation(MLEnvironmentFactory.get(getMLEnvironmentId()));
		return (T) this;
	}

	public BatchOperator <?> sample(double ratio) {
		return linkTo(new SampleBatchOp(ratio).setMLEnvironmentId(getMLEnvironmentId()));
	}

	public BatchOperator <?> sample(double ratio, boolean withReplacement) {
		return linkTo(new SampleBatchOp(ratio, withReplacement).setMLEnvironmentId(getMLEnvironmentId()));
	}

	public BatchOperator <?> sampleWithSize(int numSamples) {
		return linkTo(new SampleWithSizeBatchOp(numSamples).setMLEnvironmentId(getMLEnvironmentId()));
	}

	public BatchOperator <?> sampleWithSize(int numSamples, boolean withReplacement) {
		return linkTo(new SampleWithSizeBatchOp(numSamples, withReplacement).setMLEnvironmentId(getMLEnvironmentId()));
	}

	public BatchOperator <?> udf(String selectedColName, String outputColName, ScalarFunction scalarFunction) {
		return linkTo(
			new UDFBatchOp()
				.setSelectedCols(selectedColName)
				.setOutputCol(outputColName)
				.setFunc(scalarFunction)
				.setMLEnvironmentId(getMLEnvironmentId())
		);
	}

	public BatchOperator <?> udtf(String selectedColName, String[] outputColNames, TableFunction tableFunction) {
		return linkTo(
			new UDTFBatchOp()
				.setSelectedCols(selectedColName)
				.setOutputCols(outputColNames)
				.setFunc(tableFunction)
				.setMLEnvironmentId(getMLEnvironmentId())
		);
	}

	public BatchOperator <?> udf(String selectedColName, String outputColName, ScalarFunction scalarFunction,
								 String[] reservedColNames) {
		return linkTo(
			new UDFBatchOp()
				.setSelectedCols(selectedColName)
				.setOutputCol(outputColName)
				.setFunc(scalarFunction)
				.setReservedCols(reservedColNames)
				.setMLEnvironmentId(getMLEnvironmentId())
		);
	}

	public BatchOperator <?> udtf(String selectedColName, String[] outputColNames, TableFunction tableFunction,
								  String[] reservedColNames) {
		return linkTo(
			new UDTFBatchOp()
				.setSelectedCols(selectedColName)
				.setOutputCols(outputColNames)
				.setFunc(tableFunction)
				.setReservedCols(reservedColNames)
				.setMLEnvironmentId(getMLEnvironmentId())
		);
	}

	public FirstNBatchOp firstN(int n) {
		return linkTo(new FirstNBatchOp().setSize(n).setMLEnvironmentId(getMLEnvironmentId()));
	}

	public static ExecutionEnvironment getExecutionEnvironmentFromOps(BatchOperator <?>... ops) {
		return getExecutionEnvironment(x -> x.getDataSet().getExecutionEnvironment(), ops);
	}

	public static ExecutionEnvironment getExecutionEnvironmentFromDataSets(DataSet <?>... dataSets) {
		return getExecutionEnvironment(DataSet::getExecutionEnvironment, dataSets);
	}

	private static <T> ExecutionEnvironment getExecutionEnvironment(
		Function <T, ExecutionEnvironment> getFunction, T[] types) {
		Preconditions.checkState(types != null && types.length > 0,
			"The operators must not be empty when get ExecutionEnvironment");

		ExecutionEnvironment env = null;

		for (T type : types) {
			if (type == null) {
				continue;
			}

			ExecutionEnvironment executionEnv = getFunction.apply(type);

			if (env != null && env != executionEnv) {
				throw new AkIllegalOperationException("The operators must be running in the same "
					+ "ExecutionEnvironment");
			}

			env = executionEnv;
		}

		Preconditions.checkNotNull(env,
			"Could not find the ExecutionEnvironment in the operators. " +
				"There is a bug. Please contact the developer.");

		return env;
	}

	@IoOpAnnotation(name = "mem_batch_sink", ioType = IOType.SinkBatch)
	private static class MemSinkBatchOp extends BaseSinkBatchOp <MemSinkBatchOp> {
		private static final long serialVersionUID = -2595920715328848084L;
		private final String id = new AbstractID().toString();
		private TypeSerializer <Row> serializer;

		public MemSinkBatchOp() {
			this(new Params());
		}

		public MemSinkBatchOp(Params params) {
			super(AnnotationUtils.annotatedName(MemSinkBatchOp.class), params);
		}

		@Override
		public MemSinkBatchOp linkFrom(BatchOperator <?>... inputs) {
			return sinkFrom(checkAndGetFirst(inputs));
		}

		@Override
		protected MemSinkBatchOp sinkFrom(BatchOperator <?> in) {
			DataSet <Row> input = in.getDataSet();

			serializer = input.getType().createSerializer(
				MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment().getConfig()
			);

			input.output(new Utils.CollectHelper <>(id, serializer));

			return this;
		}

		public List <Row> getResult(JobExecutionResult res) {
			ArrayList <byte[]> accResult = res.getAccumulatorResult(id);
			if (accResult != null) {
				try {
					return SerializedListAccumulator.deserializeList(accResult, serializer);
				} catch (ClassNotFoundException e) {
					throw new AkUnclassifiedErrorException("Cannot find type class of collected data type.", e);
				} catch (IOException e) {
					throw new AkUnclassifiedErrorException("Serialization error while deserializing collected data",
						e);
				}
			} else {
				throw new AkUnclassifiedErrorException("The call to collect() could not retrieve the DataSet.");
			}
		}
	}

	public T lazyPrint() {
		return lazyPrint(0, null);
	}

	public T lazyPrint(String title) {
		return lazyPrint(0, title);
	}

	public T lazyPrint(int n) {
		return lazyPrint(n, null);
	}

	public T lazyPrint(int n, String title) {
		LazyObjectsManager lazyObjectsManager = LazyObjectsManager.getLazyObjectsManager(this);
		BatchOperator <?> op = n > 0 ? this.firstN(n) : this;
		LazyEvaluation <Pair <BatchOperator <?>, List <Row>>> lazyRowOps = lazyObjectsManager.genLazySink(op);
		lazyRowOps.addCallback(d -> {
			if (null != title) {
				System.out.println(title);
			}
			System.out.println(TableUtil.formatTitle(d.getLeft().getColNames()));

			if (0 == n) {
				List <Row> rows = d.getRight();
				if (rows.size() > 21) {
					for (int i = 0; i < 10; i++) {
						System.out.println(TableUtil.formatRows(rows.get(i)));
					}
					System.out.println(" ......");
					for (int i = rows.size() - 10; i < rows.size(); i++) {
						System.out.println(TableUtil.formatRows(rows.get(i)));
					}
					return;
				}
			}

			for (Row row : d.getRight()) {
				System.out.println(TableUtil.formatRows(row));
			}

		});
		return (T) this;
	}

	@SafeVarargs
	public final T lazyCollect(Consumer <List <Row>>... callbacks) {
		LazyObjectsManager lazyObjectsManager = LazyObjectsManager.getLazyObjectsManager(this);
		LazyEvaluation <Pair <BatchOperator <?>, List <Row>>> lazyRowOps = lazyObjectsManager.genLazySink(this);
		for (Consumer <List <Row>> callback : callbacks) {
			lazyRowOps.addCallback(d -> callback.accept(d.getRight()));
		}
		return (T) this;
	}

	public final T lazyCollectMTable(Consumer <MTable>... callbacks) {
		LazyObjectsManager lazyObjectsManager = LazyObjectsManager.getLazyObjectsManager(this);
		LazyEvaluation <Pair <BatchOperator <?>, List <Row>>> lazyRowOps = lazyObjectsManager.genLazySink(this);
		for (Consumer <MTable> callback : callbacks) {
			lazyRowOps.addCallback(d -> callback.accept(new MTable(d.getRight(), getSchema())));
		}
		return (T) this;
	}

	public final T lazyVizDive() {
		sampleWithSize(10000).lazyCollect(new DiveVisualizerConsumer(getColNames()));
		return (T) this;
	}

	public final T lazyVizStatistics() {
		return lazyVizStatistics(getOutputTable().toString());
	}

	public final T lazyVizStatistics(String tableName) {
		InternalFullStatsBatchOp internalFullStatsBatchOp = new InternalFullStatsBatchOp().linkFrom(this);
		internalFullStatsBatchOp.lazyVizFullStats(new String[] {tableName});
		//noinspection unchecked
		return (T) this;
	}

	private static void triggerLazyEvaluation(MLEnvironment mlEnv) {
		triggerLazyEvaluation(mlEnv, null);
	}

	private static void triggerLazyEvaluation(MLEnvironment mlEnv, String jobName) {
		LazyObjectsManager lazyObjectsManager = null;
		try {
			lazyObjectsManager = mlEnv.getLazyObjectsManager();
			lazyObjectsManager.checkLazyOpsAfterLinked();

			Map <BatchOperator <?>, LazyEvaluation <Pair <BatchOperator <?>, List <Row>>>> lazyRowOps
				= lazyObjectsManager.getLazySinks();
			List <BatchOperator <?>> opsToCollect = new ArrayList <>(lazyRowOps.keySet());
			List <List <Row>> listRows;
			try {
				listRows = collect(jobName, opsToCollect.toArray(new BatchOperator[0]));
			} catch (Exception e) {
				throw new AkFlinkExecutionErrorException("Failed to collect ops data.", e);
			}

			for (int i = 0; i < opsToCollect.size(); i += 1) {
				BatchOperator <?> op = opsToCollect.get(i);
				if (lazyRowOps.containsKey(op)) {
					List <Row> rows = listRows.get(i);
					lazyRowOps.get(op).addValue(Pair.of(op, rows));
				}
			}
		} finally {
			if (lazyObjectsManager != null) {
				lazyObjectsManager.clearVirtualSinks();
				lazyObjectsManager.clearLazyOpsAfterLinked();
			}
		}
	}

	public static List <List <Row>> collect(BatchOperator <?>... batchOperators) throws Exception {
		return collect(null, batchOperators);
	}

	private static List <List <Row>> collect(String jobName, BatchOperator <?>... batchOperators) throws Exception {
		List <MemSinkBatchOp> memSinks = new ArrayList <>();

		Long mlEnvId = MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID;

		for (BatchOperator <?> batchOperator : batchOperators) {
			mlEnvId = batchOperator.getMLEnvironmentId();
			MemSinkBatchOp memSinkBatchOp = new MemSinkBatchOp();
			memSinkBatchOp.setMLEnvironmentId(mlEnvId);
			batchOperator.link(memSinkBatchOp);
			memSinks.add(memSinkBatchOp);
		}

		ExecutionEnvironment env = MLEnvironmentFactory
			.get(mlEnvId)
			.getExecutionEnvironment();

		JobExecutionResult res = null == jobName
			? env.execute()
			: env.execute(jobName);

		List <List <Row>> ret = new ArrayList <>();

		for (MemSinkBatchOp memSink : memSinks) {
			ret.add(memSink.getResult(res));
		}

		return ret;
	}

	private SummarizerBatchOp getStatisticsOp() {
		SummarizerBatchOp summarizerBatchOp = new SummarizerBatchOp()
			.setMLEnvironmentId(getMLEnvironmentId());
		if (this.isNullOutputTable() && !(this instanceof BaseSourceBatchOp)) {
			LazyObjectsManager lazyObjectsManager = LazyObjectsManager.getLazyObjectsManager(this);
			LazyEvaluation <BatchOperator <?>> lazyOpAfterLinked = lazyObjectsManager.genLazyOpAfterLinked(this);
			lazyOpAfterLinked.addCallback(d -> d.link(summarizerBatchOp));
		} else {
			this.link(summarizerBatchOp);
		}
		return summarizerBatchOp;
	}

	public TableSummary collectStatistics() {
		return getStatisticsOp().collectSummary();
	}

	public T lazyCollectStatistics(Consumer <TableSummary>... callbacks) {
		return lazyCollectStatistics(Arrays.asList(callbacks));
	}

	public T lazyCollectStatistics(List <Consumer <TableSummary>> callbacks) {
		getStatisticsOp().lazyCollectSummary(callbacks);
		return (T) this;
	}

	public T lazyPrintStatistics() {
		return lazyPrintStatistics(null);
	}

	public T lazyPrintStatistics(String title) {
		return lazyCollectStatistics(d -> {
			if (null != title) {
				System.out.println(title);
			}
			System.out.println(d);
		});
	}
}
