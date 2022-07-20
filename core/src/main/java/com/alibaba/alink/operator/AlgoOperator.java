package com.alibaba.alink.operator;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.api.misc.param.WithParams;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.params.shared.HasMLEnvironmentId;

import java.io.Serializable;
import java.lang.reflect.Field;

/**
 * Base class for algorithm operators.
 *
 * <p>Base class for the algorithm operators. It hosts the parameters and output
 * tables of an algorithm operator. Each AlgoOperator may have one or more output tables.
 * One of the output table is the primary output table which can be obtained by calling
 * {@link #getOutputTable}. The other output tables are side output tables that can be obtained
 * by calling {@link #getSideOutputTables()}.
 *
 * @param <T> The class type of the {@link AlgoOperator} implementation itself
 */
public abstract class AlgoOperator<T extends AlgoOperator <T>>
	implements WithParams <T>, HasMLEnvironmentId <T>, Serializable {

	private static final long serialVersionUID = 5891637813713246040L;
	/**
	 * Params for algorithms.
	 */
	private Params params;

	/**
	 * The table held by operator.
	 */
	private Table output = null;

	/**
	 * The side outputs of operator that be similar to the stream's side outputs.
	 */
	private Table[] sideOutputs = null;

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
	protected AlgoOperator() {
		this(null);
	}

	/**
	 * Construct the operator with the initial Params.
	 */
	protected AlgoOperator(Params params) {
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
	public Table getOutputTable() {
		if (null == this.output) {
			throw new RuntimeException(
				"There is no output. Please call current BatchOperator's 'link' or related method firstly, "
					+ "or this BatchOperator has no output.");
		} else {
			return this.output;
		}
	}

	public boolean isNullOutputTable() {
		return null == this.output;
	}

	@Deprecated
	public Table getOutput() {
		return getOutputTable();
	}

	/**
	 * Returns the side outputs.
	 *
	 * @return the side outputs.
	 */
	protected Table[] getSideOutputTables() {
		return this.sideOutputs;
	}

	@Deprecated
	public Table[] getSideOutputs() {
		return getSideOutputTables();
	}

	/**
	 * Set the side outputs.
	 *
	 * @param sideOutputs the side outputs set the operator.
	 */
	protected void setSideOutputTables(Table[] sideOutputs) {
		this.sideOutputs = sideOutputs;
	}

	@Deprecated
	protected void setSideOutputs(Table[] sideOutputs) {
		setSideOutputTables(sideOutputs);
	}

	/**
	 * Set the table held by operator.
	 *
	 * @param output the output table.
	 */
	protected void setOutputTable(Table output) {
		this.output = output;
	}

	@Deprecated
	protected void setOutput(Table output) {
		this.output = output;
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
	 * Evaluate the "select" query on the AlgoOperator.
	 *
	 * @param fields The fields to select.
	 * @return The evaluation result as a AlgoOperator.
	 */
	public abstract AlgoOperator <?> select(String fields);

	/**
	 * Select some columns from the AlgoOperator.
	 *
	 * @param fields The names of the columns to select.
	 * @return The evaluation result as a AlgoOperator.
	 */
	public abstract AlgoOperator <?> select(String[] fields);

	/**
	 * Rename the columns.
	 *
	 * @param fields Comma separated column names.
	 * @return The AlgoOperator after renamed.
	 */
	public abstract AlgoOperator <?> as(String fields);

	/**
	 * Rename the columns.
	 *
	 * @param fields An array of new column names.
	 * @return The AlgoOperator after renamed.
	 */
	public abstract AlgoOperator <?> as(String[] fields);

	/**
	 * Apply the "filter" operation on the AlgoOperator.
	 *
	 * @param predicate The filter conditions.
	 * @return The filter result.
	 */
	public abstract AlgoOperator <?> filter(String predicate);

	/**
	 * Apply the "filter" operation on the AlgoOperator.
	 *
	 * @param predicate The filter conditions.
	 * @return The filter result.
	 */
	public abstract AlgoOperator <?> where(String predicate);

	protected static void checkOpSize(int size, AlgoOperator <?>... inputs) {
		AkPreconditions.checkNotNull(inputs, "Operators should not be null.");
		AkPreconditions.checkState(inputs.length == size, "The size of operators should be equal to "
			+ size + ", current: " + inputs.length);
	}

	protected static void checkMinOpSize(int size, AlgoOperator <?>... inputs) {
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

	/* open ends here */

	/**
	 * Print the data in the AlgoOperator.
	 *
	 * @return The AlgoOperator itself.
	 */
	public abstract AlgoOperator <?> print() throws Exception;

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
				throw new RuntimeException(ex);
			}
		}
	}

}
