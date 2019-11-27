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

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.batch.dataproc.FirstNBatchOp;
import com.alibaba.alink.operator.batch.dataproc.SampleBatchOp;
import com.alibaba.alink.operator.batch.dataproc.SampleWithSizeBatchOp;
import com.alibaba.alink.operator.batch.sink.BaseSinkBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.batch.utils.PrintBatchOp;
import com.alibaba.alink.operator.batch.utils.UDFBatchOp;
import com.alibaba.alink.operator.batch.utils.UDTFBatchOp;
import com.alibaba.alink.operator.common.sql.BatchSqlOperators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Base class of batch algorithm operators.
 * <p>
 * <p>This class extends {@link AlgoOperator} to support data transmission between BatchOperators.
 */
public abstract class BatchOperator<T extends BatchOperator <T>> extends AlgoOperator <T> {

	public BatchOperator() {
		super();
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
		return BatchSqlOperators.select(this, fields);
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
	public BatchOperator<?> as(String[] fields) {
		StringBuilder sbd = new StringBuilder();
		for (int i = 0; i < fields.length; i++) {
			if(i > 0) {
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
	public BatchOperator distinct() {
		return BatchSqlOperators.distinct(this);
	}

	/**
	 * Order the records by a specific field and keeping a limited number of records.
	 *
	 * @param fieldName The name of the field by which the records are ordered.
	 * @param limit     The maximum number of records to keep.
	 * @return The resulted <code>BatchOperator</code> of the "orderBy" operation.
	 */
	public BatchOperator orderBy(String fieldName, int limit) {
		return orderBy(fieldName, limit, true);
	}

	public BatchOperator orderBy(String fieldName, int limit, boolean isAscending) {
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
	public BatchOperator orderBy(String fieldName, int offset, int fetch) {
		return orderBy(fieldName, offset, fetch, true);
	}

	public BatchOperator orderBy(String fieldName, int offset, int fetch,  boolean isAscending) {
		return BatchSqlOperators.orderBy(this, fieldName, isAscending, offset, fetch);
	}

	/**
	 * Apply the "group by" operation.
	 *
	 * @param groupByPredicate The predicate specifying the fields from which records are grouped.
	 * @param selectClause     The clause specifying the fields to select and the aggregation operations.
	 * @return The resulted <code>BatchOperator</code> of the "groupBy" operation.
	 */
	public BatchOperator groupBy(String groupByPredicate, String selectClause) {
		return BatchSqlOperators.groupBy(this, groupByPredicate, selectClause);
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

	protected String tableName = null;

	public static void execute() throws Exception {
		MLEnvironmentFactory.getDefault().getExecutionEnvironment().execute();
	}

	public static void setParallelism(int parallelism) {
		MLEnvironmentFactory.getDefault().getExecutionEnvironment().setParallelism(parallelism);
	}

	public static void disableLogging() {
		MLEnvironmentFactory.getDefault().getExecutionEnvironment().getConfig().disableSysoutLogging();
	}

	//    public BatchOperator withStat() {
	//        return withStatWithLevel("L1");
	//    }

	//    public BatchOperator withStatWithLevel(String statLevel) {
	//        stat(statLevel);
	//        return this;
	//    }

	//    public BatchOperator withStat(String nodeName) {
	//        Params params = new Params().set(MLSession.getScreenManager().getVizName(), nodeName);
	//        this.link(new AllStatBatchOp(params));
	//        this.withStatWithLevel("L1");
	//        return this;
	//    }
	//
	//    public DataSet <SimpleStatResultTable> getStatResult() {
	//        return this.srtDataSet;
	//    }

	//    /**
	//     * @param statLevel: L1,L2,L3: 默认是L1
	//     *                   L1 has basic statistic;
	//     *                   L2 has simple statistic and cov/corr;
	//     *                   L3 has simple statistic, cov/corr, histogram, freq, topk, bottomk;
	//     */
	//    private void stat(String statLevel) {
	//        if (null != getTable()) {
	//            this.srtDataSet = RowTypeDataSet.fromTable(getMLEnvironmentId(), getTable())
	//                .mapPartition(new SimpleStatPartition(getTable().getSchema(), statLevel))
	//                .reduce(new ReduceFunction <SimpleStatResultTable>() {
	//                    @Override
	//                    public SimpleStatResultTable reduce(SimpleStatResultTable a, SimpleStatResultTable b) {
	//                        if (null == a) {
	//                            return b;
	//                        } else if (null == b) {
	//                            return a;
	//                        } else {
	//                            try {
	//                                return SimpleStatResultTable.combine(a, b);
	//                            } catch (Exception ex) {
	//                                ex.printStackTrace();
	//                                return null;
	//                            }
	//                        }
	//                    }
	//                });
	//        }
	//    }

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
		try {
			return getDataSet().collect();
		} catch (Exception e) {
			throw new RuntimeException("Collect result fail in BatchOperator.", e);
		}
	}

	public String getTableName() {
		if (null == tableName) {
			tableName = getOutputTable().toString();
		}
		return tableName;
	}

	public BatchOperator setTableName(String name) {
		MLEnvironmentFactory.get(getMLEnvironmentId()).getBatchTableEnvironment().registerTable(name, getOutputTable());
		this.tableName = name;
		return this;
	}

	public BatchOperator forceSetTableName(String name) {
		//        BatchTableEnvironment env = MLEnvironmentFactory.get(getMLEnvironmentId())
		// .getBatchTableEnvironment();
		//        Catalog catalog = env.getCatalog(env.getCurrentCatalog()).get();
		//        ObjectPath tablePath = new ObjectPath(catalog.getDefaultDatabase(), name);
		//        try {
		//            catalog.dropTable(tablePath, true);
		//        } catch (Exception ex) {
		//            throw new RuntimeException(ex);
		//        }
		//        this.tableName = name;
		//        return this;
		return setTableName(name);
	}

	public BatchOperator getSideOutput(int idx) {
		if (null == this.getSideOutputTables()) {
			throw new RuntimeException("There is no side output.");
		} else if (idx < 0 || idx >= this.getSideOutputTables().length) {
			throw new RuntimeException("There is no  side output.");
		} else {
			return new TableSourceBatchOp(this.getSideOutputTables()[idx]).setMLEnvironmentId(getMLEnvironmentId());
		}
	}

	public int getSideOutputCount() {
		return null == this.getSideOutputTables() ? 0 : this.getSideOutputTables().length;
	}

	@Override
	public BatchOperator print() throws Exception {
		return linkTo(new PrintBatchOp().setMLEnvironmentId(getMLEnvironmentId()));
	}

	public BatchOperator sample(double ratio) {
		return linkTo(new SampleBatchOp(ratio).setMLEnvironmentId(getMLEnvironmentId()));
	}

	public BatchOperator sample(double ratio, boolean withReplacement) {
		return linkTo(new SampleBatchOp(ratio, withReplacement).setMLEnvironmentId(getMLEnvironmentId()));
	}

	public BatchOperator sampleWithSize(int numSamples) {
		return linkTo(new SampleWithSizeBatchOp(numSamples).setMLEnvironmentId(getMLEnvironmentId()));
	}

	public BatchOperator sampleWithSize(int numSamples, boolean withReplacement) {
		return linkTo(new SampleWithSizeBatchOp(numSamples, withReplacement).setMLEnvironmentId(getMLEnvironmentId()));
	}

	public BatchOperator udf(String selectedColName, String outputColName, ScalarFunction scalarFunction) {
		return linkTo(
			new UDFBatchOp()
				.setSelectedCols(selectedColName)
				.setOutputCol(outputColName)
				.setFunc(scalarFunction)
				.setMLEnvironmentId(getMLEnvironmentId())
		);
	}

	public BatchOperator udtf(String selectedColName, String[] outputColNames, TableFunction tableFunction) {
		return linkTo(
			new UDTFBatchOp()
				.setSelectedCols(selectedColName)
				.setOutputCols(outputColNames)
				.setFunc(tableFunction)
				.setMLEnvironmentId(getMLEnvironmentId())
		);
	}

	public BatchOperator udf(String selectedColName, String outputColName, ScalarFunction scalarFunction,
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

	public BatchOperator udtf(String selectedColName, String[] outputColNames, TableFunction tableFunction,
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
		return linkTo(new FirstNBatchOp(n).setMLEnvironmentId(getMLEnvironmentId()));
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
				throw new RuntimeException("The operators must be runing in the same ExecutionEnvironment");
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
		private final String id = new AbstractID().toString();
		private TypeSerializer <Row> serializer;

		public MemSinkBatchOp() {
			this(new Params());
		}

		public MemSinkBatchOp(Params params) {
			super(AnnotationUtils.annotatedName(MemSinkBatchOp.class), params);
		}

		@Override
		protected MemSinkBatchOp sinkFrom(BatchOperator in) {
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
					throw new RuntimeException("Cannot find type class of collected data type.", e);
				} catch (IOException e) {
					throw new RuntimeException("Serialization error while deserializing collected data", e);
				}
			} else {
				throw new RuntimeException("The call to collect() could not retrieve the DataSet.");
			}
		}
	}

	public static List <List <Row>> collect(BatchOperator <?>... batchOperators) throws Exception {
		List <MemSinkBatchOp> memSinks = new ArrayList <>();

		Long mlEnvId = MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID;

		for (BatchOperator <?> batchOperator : batchOperators) {
			mlEnvId = batchOperator.getMLEnvironmentId();
			MemSinkBatchOp memSinkBatchOp = new MemSinkBatchOp();
			memSinkBatchOp.setMLEnvironmentId(mlEnvId);
			batchOperator.link(memSinkBatchOp);
			memSinks.add(memSinkBatchOp);
		}

		JobExecutionResult res = MLEnvironmentFactory
			.get(mlEnvId)
			.getExecutionEnvironment()
			.execute();

		List <List <Row>> ret = new ArrayList <>();

		for (MemSinkBatchOp memSink : memSinks) {
			ret.add(memSink.getResult(res));
		}

		return ret;
	}
}
