package com.alibaba.alink.operator.local;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.api.misc.param.WithParams;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.LocalMLEnvironment;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.exceptions.AkIllegalOperationException;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.lazy.LazyEvaluation;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.utils.DiveVisualizer.DiveVisualizerConsumer;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.operator.local.lazy.LocalLazyObjectsManager;
import com.alibaba.alink.operator.local.source.BaseSourceLocalOp;
import com.alibaba.alink.operator.local.source.TableSourceLocalOp;
import com.alibaba.alink.operator.local.sql.GroupByLocalOp;
import com.alibaba.alink.operator.local.statistics.SummarizerLocalOp;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;

public abstract class LocalOperator<T extends LocalOperator <T>>
	implements WithParams <T>, Serializable {

	private final Params params;

	/**
	 * The table held by operator.
	 */
	private MTable output = null;

	/**
	 * The side outputs of operator that be similar to the stream's side outputs.
	 */
	private MTable[] sideOutputs = null;

	/**
	 * Construct the operator with empty Params.
	 *
	 * <p>This constructor is especially useful when users want to set parameters
	 * for the algorithm operators. For example:
	 * SplitBatchOp is widely used in ML data pre-processing,
	 * which splits one dataset into two dataset: training set and validation set.
	 * It is very convenient for us to write code like this:
	 * <pre>
	 * {@code
	 * new SplitBatchOp().setSplitRatio(0.9)
	 * }
	 * </pre>
	 */
	protected LocalOperator() {
		this(null);
	}

	/**
	 * Construct the operator with the initial Params.
	 */
	protected LocalOperator(Params params) {
		if (null == params) {
			this.params = new Params();
		} else {
			this.params = params.clone();
		}
		checkDefaultParameters();
	}

	@Override
	public Params getParams() {
		return this.params;
	}

	/**
	 * Returns the table held by operator.
	 *
	 * @return the table
	 */
	public MTable getOutputTable() {
		if (null == this.output) {
			throw new AkIllegalOperationException(
				"There is no output. Please call current LocalOperator's 'link' or related method firstly, "
					+ "or this LocalOperator has no output.");
		} else {
			return this.output;
		}
	}

	public boolean isNullOutputTable() {
		return null == this.output;
	}

	@Deprecated
	public MTable getOutput() {
		return getOutputTable();
	}

	/**
	 * Returns the side outputs.
	 *
	 * @return the side outputs.
	 */
	protected MTable[] getSideOutputTables() {
		return this.sideOutputs;
	}

	@Deprecated
	public MTable[] getSideOutputs() {
		return getSideOutputTables();
	}

	/**
	 * Set the side outputs.
	 *
	 * @param sideOutputs the side outputs set the operator.
	 */
	protected void setSideOutputTables(MTable[] sideOutputs) {
		this.sideOutputs = sideOutputs;
	}

	@Deprecated
	protected void setSideOutputs(MTable[] sideOutputs) {
		setSideOutputTables(sideOutputs);
	}

	/**
	 * Set the table held by operator.
	 *
	 * @param output the output table.
	 */
	protected void setOutputTable(MTable output) {
		this.output = output;
	}

	public List <Row> collect() {
		LocalMLEnvironment mlEnv = LocalMLEnvironment.getInstance();
		LazyEvaluation <Pair <LocalOperator <?>, List <Row>>> lazyRows = mlEnv.getLazyObjectsManager()
			.genLazySink(this);
		triggerLazyEvaluation(mlEnv);
		return lazyRows.getLatestValue().getRight();
	}

	/**
	 * Get the column names of the output table.
	 *
	 * @return the column names.
	 */
	public String[] getColNames() {
		return getSchema().getFieldNames().clone();
	}

	/**
	 * Get the column types of the output table.
	 *
	 * @return the column types.
	 */
	public TypeInformation <?>[] getColTypes() {
		return getSchema().getFieldTypes().clone();
	}

	/**
	 * Get the column names of the specified side-output table.
	 *
	 * @param index the index of the table.
	 * @return the column types of the table.
	 */
	@Deprecated
	public String[] getSideOutputColNames(int index) {
		checkSideOutputAccessibility(index);
		return sideOutputs[index].getSchema().getFieldNames().clone();
	}

	/**
	 * Get the column types of the specified side-output table.
	 *
	 * @param index the index of the table.
	 * @return the column types of the table.
	 */
	@Deprecated
	public TypeInformation <?>[] getSideOutputColTypes(int index) {
		checkSideOutputAccessibility(index);
		return sideOutputs[index].getSchema().getFieldTypes().clone();
	}

	/**
	 * Get the schema of the output table.
	 *
	 * @return the schema.
	 */
	public TableSchema getSchema() {
		return this.getOutputTable().getSchema();
	}

	/**
	 * Returns the name of output table.
	 *
	 * @return the name of output table.
	 */
	@Override
	public String toString() {
		return getOutputTable().toString();
	}

	/**
	 * Link to another {@link LocalOperator}.
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
	 * @see #linkFrom(LocalOperator[])
	 */
	public <B extends LocalOperator <?>> B link(B next) {
		next.linkFrom(this);
		return next;
	}

	/**
	 * Link from others {@link LocalOperator}.
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
	public abstract T linkFrom(LocalOperator <?>... inputs);

	public LocalOperator <?> select(String fields) {
		return LocalMLEnvironment.getInstance().getSqlExecutor().select(this, fields);
	}

	public LocalOperator <?> select(String[] fields) {
		return new TableSourceLocalOp(this.getOutputTable().select(fields));
	}

	public LocalOperator <?> as(String fields) {
		return LocalMLEnvironment.getInstance().getSqlExecutor().as(this, fields);
	}

	public LocalOperator <?> as(String[] fields) {
		StringBuilder sbd = new StringBuilder();
		for (int i = 0; i < fields.length; i++) {
			if (i > 0) {
				sbd.append(",");
			}
			sbd.append(fields[i]);
		}
		return as(sbd.toString());
	}

	public LocalOperator <?> where(String predicate) {
		return LocalMLEnvironment.getInstance().getSqlExecutor().where(this, predicate);
	}

	public LocalOperator <?> filter(String predicate) {
		return LocalMLEnvironment.getInstance().getSqlExecutor().filter(this, predicate);
	}

	/**
	 * Remove duplicated records.
	 *
	 * @return The resulted <code>BatchOperator</code> of the "distinct" operation.
	 */
	public LocalOperator <?> distinct() {
		return LocalMLEnvironment.getInstance().getSqlExecutor().distinct(this);
	}

	public LocalOperator <?> orderBy(String fieldName, int limit, boolean isAscending) {
		return LocalMLEnvironment.getInstance().getSqlExecutor()
			.orderBy(this, fieldName, isAscending, limit);
	}

	/**
	 * Order the records by a specific field and keeping a specific range of records.
	 *
	 * @param fieldName The name of the field by which the records are ordered.
	 * @param offset    The starting position of records to keep.
	 * @param fetch     The  number of records to keep.
	 * @return The resulted <code>BatchOperator</code> of the "orderBy" operation.
	 */
	public LocalOperator <?> orderBy(String fieldName, int offset, int fetch) {
		return orderBy(fieldName, offset, fetch, true);
	}

	public LocalOperator <?> orderBy(String fieldName, int offset, int fetch, boolean isAscending) {
		return LocalMLEnvironment.getInstance().getSqlExecutor()
			.orderBy(this, fieldName, isAscending, offset, fetch);
	}

	/**
	 * Order the records by a specific field and keeping a limited number of records.
	 *
	 * @param fieldName The name of the field by which the records are ordered.
	 * @param limit     The maximum number of records to keep.
	 * @return The resulted <code>BatchOperator</code> of the "orderBy" operation.
	 */
	public LocalOperator <?> orderBy(String fieldName, int limit) {
		return orderBy(fieldName, limit, true);
	}

	/**
	 * Apply the "group by" operation.
	 *
	 * @param groupByPredicate The predicate specifying the fields from which records are grouped.
	 * @param selectClause     The clause specifying the fields to select and the aggregation operations.
	 * @return The resulted <code>BatchOperator</code> of the "groupBy" operation.
	 */
	public LocalOperator <?> groupBy(String groupByPredicate, String selectClause) {
		return new GroupByLocalOp(groupByPredicate, selectClause).linkFrom(this);
	}

	protected static LocalOperator <?> checkAndGetFirst(LocalOperator <?>... inputs) {
		checkOpSize(1, inputs);
		return inputs[0];
	}

	protected static void checkOpSize(int size, LocalOperator <?>... inputs) {
		AkPreconditions.checkNotNull(inputs, "Operators should not be null.");
		AkPreconditions.checkState(inputs.length == size, "The size of operators should be equal to "
			+ size + ", current: " + inputs.length);
	}

	protected static void checkMinOpSize(int size, LocalOperator <?>... inputs) {
		AkPreconditions.checkNotNull(inputs, "Operators should not be null.");
		AkPreconditions.checkState(inputs.length >= size, "The size of operators should be equal or greater than "
			+ size + ", current: " + inputs.length);
	}

	@Deprecated
	private void checkSideOutputAccessibility(int index) {
		AkPreconditions.checkNotNull(sideOutputs,
			"There is not side-outputs in this AlgoOperator.");
		AkPreconditions.checkState(index >= 0 && index < sideOutputs.length,
			String.format("The index(%s) of side-outputs is out of bound.", index));
		AkPreconditions.checkNotNull(sideOutputs[index],
			String.format("The %snd of side-outputs is null. Maybe the operator has not been linked.", index));
	}

	public LocalOperator <?> sampleWithSize(int numSamples) {
		return new TableSourceLocalOp(getOutputTable().sampleWithSize(numSamples, new Random()));
	}

	/**
	 * Register the table of this operator the default {@link LocalMLEnvironment}. An operator can register multiple
	 * times with different names.
	 *
	 * @param name The name to register with.
	 * @return This operator.
	 */
	public LocalOperator <?> registerTableName(String name) {
		LocalMLEnvironment.getInstance().getSqlExecutor().addTable(name, this);
		return this;
	}

	public static void removeTableName(String name) {
		LocalMLEnvironment.getInstance().getSqlExecutor().removeTable(name);
	}

	public static void registerFunction(String name, ScalarFunction function) {
		LocalMLEnvironment.getInstance().getSqlExecutor().addFunction(name, function);
	}

	public static void registerFunction(String name, TableFunction <Row> function) {
		LocalMLEnvironment.getInstance().getSqlExecutor().addFunction(name, function);
	}

	/**
	 * Evaluate SQL queries within the default {@link LocalMLEnvironment}.
	 *
	 * @param query The query to evaluate.
	 * @return The evaluation result returned as a {@link LocalOperator}.
	 */
	public static LocalOperator <?> sqlQuery(String query) {
		return LocalMLEnvironment.getInstance().getSqlExecutor().query(query);
	}

	public static void execute() {
		triggerLazyEvaluation(LocalMLEnvironment.getInstance());
	}

	private static void triggerLazyEvaluation(LocalMLEnvironment env) {
		LocalLazyObjectsManager lazyObjectsManager = null;
		try {
			lazyObjectsManager = env.getLazyObjectsManager();
			lazyObjectsManager.checkLazyOpsAfterLinked();

			Map <LocalOperator <?>, LazyEvaluation <Pair <LocalOperator <?>, List <Row>>>> lazyRowOps
				= lazyObjectsManager.getLazySinks();
			List <LocalOperator <?>> opsToCollect = new ArrayList <>(lazyRowOps.keySet());
			List <List <Row>> listRows = new ArrayList <>();
			for (LocalOperator <?> op : opsToCollect) {
				listRows.add(op.getOutputTable().getRows());
			}

			for (int i = 0; i < opsToCollect.size(); i += 1) {
				LocalOperator <?> op = opsToCollect.get(i);
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
		LocalLazyObjectsManager lazyObjectsManager = LocalLazyObjectsManager.getLazyObjectsManager(this);
		LocalOperator <?> op = n > 0 ? this.firstN(n) : this;
		LazyEvaluation <Pair <LocalOperator <?>, List <Row>>> lazyRowOps = lazyObjectsManager.genLazySink(op);
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
		LocalLazyObjectsManager lazyObjectsManager = LocalLazyObjectsManager.getLazyObjectsManager(this);
		LazyEvaluation <Pair <LocalOperator <?>, List <Row>>> lazyRowOps = lazyObjectsManager.genLazySink(this);
		for (Consumer <List <Row>> callback : callbacks) {
			lazyRowOps.addCallback(d -> callback.accept(d.getRight()));
		}
		return (T) this;
	}

	private LocalOperator <?> firstN(int n) {
		MTable mtable = this.getOutputTable();
		return new TableSourceLocalOp(mtable.subTable(0, Math.min(n, mtable.getNumRow())));
	}

	public T print() {
		return print(0);
	}

	public T print(String title) {
		return print(0, title);
	}

	public T print(int n) {
		return print(n, null);
	}

	public T print(int n, String title) {
		this.lazyPrint(n, title);
		triggerLazyEvaluation(LocalMLEnvironment.getInstance());
		//noinspection unchecked
		return (T) this;
	}

	public T vizDive() {
		new DiveVisualizerConsumer(getColNames())
			.accept(getOutputTable().sampleWithSize(10000, new Random()).getRows());
		return (T) this;
	}

	private SummarizerLocalOp getStatisticsOp() {
		SummarizerLocalOp summarizerBatchOp = new SummarizerLocalOp();
		if (this.isNullOutputTable() && !(this instanceof BaseSourceLocalOp)) {
			LocalLazyObjectsManager lazyObjectsManager = LocalLazyObjectsManager.getLazyObjectsManager(this);
			LazyEvaluation <LocalOperator <?>> lazyOpAfterLinked = lazyObjectsManager.genLazyOpAfterLinked(this);
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

	public T printStatistics() {
		this.lazyPrintStatistics();
		triggerLazyEvaluation(LocalMLEnvironment.getInstance());
		return (T) this;
	}

	public T printStatistics(String title) {
		this.lazyPrintStatistics(title);
		triggerLazyEvaluation(LocalMLEnvironment.getInstance());
		return (T) this;
	}

	protected void checkDefaultParameters() {
		Field[] fields = getClass().getFields();
		for (Field field : fields) {
			try {
				Object obj = field.get(this);
				if (obj instanceof ParamInfo <?>) {
					ParamInfo <?> paramInfo = (ParamInfo <?>) obj;
					if (this.params.contains(paramInfo)) {
						get((ParamInfo <?>) obj);
					}
				}
			} catch (Exception ex) {
				throw new AkIllegalOperatorParameterException(ex.getMessage());
			}
		}
	}

	public static void setDefaultNumThreads(int numThreads) {
		AlinkLocalSession.setDefaultNumThreads(numThreads);
	}

	public static int getDefaultNumThreads() {
		return AlinkLocalSession.getDefaultNumThreads();
	}
}
