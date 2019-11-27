package com.alibaba.alink.operator.stream;

import java.util.List;
import java.util.function.Function;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.common.sql.StreamSqlOperators;
import com.alibaba.alink.operator.stream.dataproc.SampleStreamOp;
import com.alibaba.alink.operator.stream.source.TableSourceStreamOp;
import com.alibaba.alink.operator.stream.utils.PrintStreamOp;
import com.alibaba.alink.operator.stream.utils.UDFStreamOp;
import com.alibaba.alink.operator.stream.utils.UDTFStreamOp;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;


/**
 * Base class of stream algorithm operators.
 * <p>
 * <p>This class extends {@link AlgoOperator} to support data transmission between StreamOperator.
 */
public abstract class StreamOperator<T extends StreamOperator <T>> extends AlgoOperator <T> {

	public StreamOperator() {
		super();
	}

	/**
	 * The constructor of StreamOperator with {@link Params}.
	 *
	 * @param params the initial Params.
	 */
	public StreamOperator(Params params) {
		super(params);
	}

	/**
	 * Link to another {@link StreamOperator}.
	 * <p>
	 * <p>Link the <code>next</code> StreamOperator using this StreamOperator as its input.
	 * <p>
	 * <p>For example:
	 * <p>
	 * <pre>
	 * {@code
	 * StreamOperator a = ...;
	 * StreamOperator b = ...;
	 *
	 * StreamOperator c = a.link(b)
	 * }
	 * </pre>
	 * <p>
	 * <p>The StreamOperator <code>c</code> in the above code
	 * is the same instance as <code>b</code> which takes
	 * <code>a</code> as its input.
	 * Note that StreamOperator <code>b</code> will be changed
	 * to link from StreamOperator <code>a</code>.
	 *
	 * @param next the linked StreamOperator
	 * @param <S>  type of StreamOperator returned
	 * @return the linked next
	 * @see #linkFrom(StreamOperator[])
	 */
	public <S extends StreamOperator <?>> S link(S next) {
		next.linkFrom(this);
		return next;
	}

	/**
	 * Link from others {@link StreamOperator}.
	 * <p>
	 * <p>Link this object to StreamOperator using the StreamOperators as its input.
	 * <p>
	 * <p>For example:
	 * <p>
	 * <pre>
	 * {@code
	 * StreamOperator a = ...;
	 * StreamOperator b = ...;
	 * StreamOperator c = ...;
	 *
	 * StreamOperator d = c.linkFrom(a, b)
	 * }
	 * </pre>
	 * <p>
	 * <p>The <code>d</code> in the above code is the same
	 * instance as StreamOperator <code>c</code> which takes
	 * both <code>a</code> and <code>b</code> as its input.
	 * <p>
	 * <p>note: It is not recommended to linkFrom itself or linkFrom the same group inputs twice.
	 *
	 * @param inputs the linked inputs
	 * @return the linked this object
	 */
	public abstract T linkFrom(StreamOperator <?>... inputs);

	/**
	 * create a new StreamOperator from table.
	 *
	 * @param table the input table
	 * @return the new StreamOperator
	 */
	public static StreamOperator <?> fromTable(Table table) {
		return new TableSourceStreamOp(table);
	}

	protected static StreamOperator <?> checkAndGetFirst(StreamOperator <?>... inputs) {
		checkOpSize(1, inputs);
		return inputs[0];
	}

	/**
	 * Get the {@link DataStream} that casted from the output table with the type of {@link Row}.
	 *
	 * @return the casted {@link DataStream}
	 */
	public DataStream <Row> getDataStream() {
		return DataStreamConversionUtil.fromTable(getMLEnvironmentId(), getOutputTable());
	}

	@Override
	public StreamOperator select(String fields) {
		return StreamSqlOperators.select(this, fields);
	}

	@Override
	public StreamOperator select(String[] fields) {
		return select(TableUtil.columnsToSqlClause(fields));
	}

	@Override
	public StreamOperator as(String fields) {
		return StreamSqlOperators.as(this, fields);
	}

    @Override
    public StreamOperator<?> as(String[] fields) {
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
//<<<<<<< HEAD
	public StreamOperator where(String clause) {
		return StreamSqlOperators.where(this, clause);
	}

	@Override
	public StreamOperator filter(String clause) {
		return StreamSqlOperators.filter(this, clause);
	}

    /* open ends here */

	/**
	 * Abbreviation of {@link #linkTo(StreamOperator)}
	 */
	public <S extends StreamOperator <?>> S linkTo(S next) {
		return link(next);
	}

	public T linkFrom(List <StreamOperator <?>> ins) {
		return linkFrom(ins.toArray(new StreamOperator <?>[0]));
	}

	protected String tableName = null;

	@Override
	public StreamOperator print() {
		return print(-1, 100);
	}

    public StreamOperator print(int refreshInterval, int maxLimit){
	    return linkTo(new PrintStreamOp(
	        new Params()
                .set(PrintStreamOp.REFRSH_INTERVAL, refreshInterval)
                .set(PrintStreamOp.MAX_LIMIT, maxLimit))
			.setMLEnvironmentId(getMLEnvironmentId()));
    }

	public static String createUniqueTableName() {
		return TableUtil.getTempTableName();
	}

	public static JobExecutionResult execute() throws Exception {
		//todo
		return MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
	}

	public static JobExecutionResult execute(String string) throws Exception {
		return MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute(string);
	}

	public static void setParallelism(int parallelism) {
		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().setParallelism(parallelism);
	}

	public static void setCheckPointConf() {
		StreamExecutionEnvironment env = MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment();

		// start a checkpoint every 30 min
		env.enableCheckpointing(1800 * 1000L);

		// advanced options:

		// set mode to exactly-once (this is the default)
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

		// make sure 500 ms of progress happen between checkpoints
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

		// checkpoints have to complete within one 30 min, or are discarded
		env.getCheckpointConfig().setCheckpointTimeout(1800 * 1000L);

		// allow only one checkpoint to be in progress at the same time
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
		//
		//// enable externalized checkpoints which are retained after job cancellation
		//env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup
		// .RETAIN_ON_CANCELLATION);
	}

	protected void setOutput(DataStream <Row> dataSet, TableSchema schema) {
		setOutputTable(DataStreamConversionUtil.toTable(getMLEnvironmentId(), dataSet, schema));
	}

	protected void setOutput(DataStream <Row> dataSet, String[] colNames) {
		setOutputTable(DataStreamConversionUtil.toTable(getMLEnvironmentId(), dataSet, colNames));
	}

	protected void setOutput(DataStream <Row> dataSet, String[] colNames, TypeInformation <?>[] colTypes) {
		setOutputTable(DataStreamConversionUtil.toTable(getMLEnvironmentId(), dataSet, colNames, colTypes));
	}

	public String getTableName() {
		if (null == tableName) {
			tableName = getOutputTable().toString();
		}
		return tableName;
	}

	public StreamOperator setTableName(String name) {
		MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamTableEnvironment().registerTable(name, getOutputTable());
		this.tableName = name;
		return this;
	}

	// FIXME
	public StreamOperator forceSetTableName(String name) {
		//        if (MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamTableEnvironment().isRegistered(name)) {
		//            MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamTableEnvironment()
		// .replaceRegisteredTable(name,
		//                new RelTable(getOutput().getRelNode()));
		//        } else {
		//            MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamTableEnvironment().registerTable(name,
        // getOutput
		//                ());
		//        }
		//todo: check the usage of replaceRegisteredTable.
		return setTableName(name);
	}

	public StreamOperator sample(double ratio) {
		return linkTo(new SampleStreamOp(ratio).setMLEnvironmentId(getMLEnvironmentId()));
	}

	public StreamOperator getSideOutput(int idx) {
		if (null == this.getSideOutputTables()) {
			throw new RuntimeException("There is no side output.");
		} else if (idx < 0 && idx >= this.getSideOutputTables().length) {
			throw new RuntimeException("There is no  side output.");
		} else {
			return new TableSourceStreamOp(this.getSideOutputTables()[idx]).setMLEnvironmentId(getMLEnvironmentId());
		}
	}

	public int getSideOutputCount() {
		return null == this.getSideOutputTables() ? 0 : this.getSideOutputTables().length;
	}

	public StreamOperator udf(String selectedColName, String outputColName, ScalarFunction scalarFunction) {
		return linkTo(
			new UDFStreamOp()
				.setSelectedCols(selectedColName)
				.setOutputCol(outputColName)
				.setFunc(scalarFunction)
				.setMLEnvironmentId(getMLEnvironmentId())
		);
	}

	public StreamOperator udtf(String selectedColName, String[] outputColNames, TableFunction tableFunction) {
		return linkTo(
			new UDTFStreamOp()
				.setSelectedCols(selectedColName)
				.setOutputCols(outputColNames)
				.setFunc(tableFunction)
				.setMLEnvironmentId(getMLEnvironmentId())
		);
	}

	public StreamOperator udf(String selectedColName, String outputColName, ScalarFunction scalarFunction,
							  String[] reservedColNames) {
		return linkTo(
			new UDFStreamOp()
				.setSelectedCols(selectedColName)
				.setOutputCol(outputColName)
				.setFunc(scalarFunction)
				.setReservedCols(reservedColNames)
				.setMLEnvironmentId(getMLEnvironmentId())
		);
	}

	public StreamOperator udtf(String selectedColName, String[] outputColNames, TableFunction tableFunction,
							   String[] reservedColNames) {
		return linkTo(
			new UDTFStreamOp()
				.setSelectedCols(selectedColName)
				.setOutputCols(outputColNames)
				.setFunc(tableFunction)
				.setReservedCols(reservedColNames)
				.setMLEnvironmentId(getMLEnvironmentId())
		);
	}

	public static StreamExecutionEnvironment getExecutionEnvironmentFromOps(StreamOperator <?>... ops) {
		return getExecutionEnvironment(x -> x.getDataStream().getExecutionEnvironment(), ops);
	}

	public static StreamExecutionEnvironment getExecutionEnvironmentFromDataStreams(DataStream <?>... dataStreams) {
		return getExecutionEnvironment(DataStream::getExecutionEnvironment, dataStreams);
	}

	private static <T> StreamExecutionEnvironment getExecutionEnvironment(
		Function <T, StreamExecutionEnvironment> getFunction, T[] types) {
		Preconditions.checkState(types != null && types.length > 0,
			"The operators must not be empty when get StreamExecutionEnvironment");

		StreamExecutionEnvironment env = null;

		for (T type : types) {
			if (type == null) {
				continue;
			}

			StreamExecutionEnvironment executionEnv = getFunction.apply(type);

			if (env != null && env != executionEnv) {
				throw new RuntimeException("The operators must be runing in the same StreamExecutionEnvironment");
			}

			env = executionEnv;
		}

		Preconditions.checkNotNull(env,
			"Could not find the StreamExecutionEnvironment in the operators. " +
				"There is a bug. Please contact the developer.");

		return env;
	}
}
