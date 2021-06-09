package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.params.recommendation.BaseItemsPerUserRecommParams;
import com.alibaba.alink.params.recommendation.BaseRecommParams;
import com.alibaba.alink.params.recommendation.BaseUsersPerItemRecommParams;

import java.io.Serializable;
import java.util.List;

/**
 * Abstract class for Recommenders. RecommKernel provides the common methods of recommendation.
 */
public abstract class RecommKernel implements Serializable {
	private static final long serialVersionUID = 377536139766639654L;
	/**
	 * Field names of the model.
	 */
	private final String[] modelFieldNames;

	/**
	 * Field types of the model.
	 */
	private final DataType[] modelFieldTypes;

	/**
	 * schema of the input.
	 */
	private final String[] dataFieldNames;
	private final DataType[] dataFieldTypes;

	/**
	 * params used for FlatMapper. User can set the params before that the FlatMapper is executed.
	 */
	protected Params params;

	protected final RecommType recommType;

	protected String userColName;
	protected String itemColName;

	private final OutputColsHelper outputColsHelper4Rate;
	private final OutputColsHelper outputColsHelper4RecommObjs;

	public RecommKernel(TableSchema modelSchema, TableSchema dataSchema, Params params, RecommType recommType) {
		this.modelFieldNames = modelSchema.getFieldNames();
		this.modelFieldTypes = modelSchema.getFieldDataTypes();
		this.dataFieldNames = dataSchema.getFieldNames();
		this.dataFieldTypes = dataSchema.getFieldDataTypes();
		this.params = (null == params) ? new Params() : params.clone();
		this.recommType = recommType;
		String resultColName = this.params.get(BaseRecommParams.RECOMM_COL);
		String[] reservedColNames = this.params.get(BaseRecommParams.RESERVED_COLS);
		this.outputColsHelper4Rate
			= new OutputColsHelper(dataSchema, resultColName, Types.DOUBLE(), reservedColNames);
		this.outputColsHelper4RecommObjs
			= new OutputColsHelper(dataSchema, resultColName, Types.STRING(), reservedColNames);
	}

	protected TableSchema getModelSchema() {
		return TableSchema.builder().fields(this.modelFieldNames, this.modelFieldTypes).build();
	}

	/**
	 * Load model from the list of Row type data.
	 *
	 * @param modelRows the list of Row type data
	 */
	public abstract void loadModel(List <Row> modelRows);

	protected TableSchema getDataSchema() {
		return TableSchema.builder().fields(dataFieldNames, dataFieldTypes).build();
	}

	/**
	 * Return the recommend result for the input info.
	 *
	 * @param input the input data
	 * @return the recommend result with Row type
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation to fail.
	 */
	public Object recommend(Object[] input) throws Exception {
		switch (this.recommType) {
			case RATE:
				return rate(input);
			case ITEMS_PER_USER:
				return recommendItemsPerUser(input[0]);
			case USERS_PER_ITEM:
				return recommendUsersPerItem(input[0]);
			case SIMILAR_ITEMS:
				return recommendSimilarItems(input[0]);
			case SIMILAR_USERS:
				return recommendSimilarUsers(input[0]);
			default:
				throw new IllegalArgumentException("NOT supported recommend type : " + recommType);
		}
	}

	/**
	 * Return the rating result for the user info and item info.
	 *
	 * @param infoUserItem the input Row type data of the user info and item info
	 * @return the rating result with Double type
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation to fail.
	 */
	abstract Double rate(Object[] infoUserItem) throws Exception;

	/**
	 * Recommend items observed for the input user.
	 *
	 * @param infoUser The input user info.
	 * @return Json string of the result values.
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation to fail.
	 */
	abstract String recommendItemsPerUser(Object infoUser) throws Exception;

	/**
	 * Recommend users observed for the input item.
	 *
	 * @param infoItem The input item info.
	 * @return Json string of the result values.
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation to fail.
	 */
	abstract String recommendUsersPerItem(Object infoItem) throws Exception;

	/**
	 * Recommend the k most similar items for the input item.
	 *
	 * @param infoItem The input item info.
	 * @return Json string of the result values.
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation to fail.
	 */
	abstract String recommendSimilarItems(Object infoItem) throws Exception;

	/**
	 * Recommend the k most similar users for the input user.
	 *
	 * @param infoUser The input user info.
	 * @return Json string of the result values.
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation to fail.
	 */
	abstract String recommendSimilarUsers(Object infoUser) throws Exception;

	/**
	 * Get the table schema(includes column names and types) of the calculation result.
	 *
	 * @return the table schema of output Row type data
	 */
	public TableSchema getOutputSchema() {
		if (RecommType.RATE == this.recommType) {
			return outputColsHelper4Rate.getResultSchema();
		} else {
			return outputColsHelper4RecommObjs.getResultSchema();
		}
	}

	public String getObjectName() {
		if (recommType == RecommType.ITEMS_PER_USER) {
			return itemColName;
		} else if (recommType == RecommType.USERS_PER_ITEM) {
			return userColName;
		} else if (recommType == RecommType.SIMILAR_USERS) {
			return userColName;
		} else if (recommType == RecommType.SIMILAR_ITEMS) {
			return itemColName;
		} else {
			throw new RuntimeException("not support yet.");
		}
	}


	/**
	 * Return a copy of 'this' object that is used in multi-threaded prediction.
	 * A rule of thumb is to share model data with the mirrored object, but not
	 * to share runtime buffer.
	 *
	 * If the ReommKernel is thread-safe (no runtime buffer), then just return 'this' is enough.
	 */
	//protected RecommKernel mirror() {
	//	return this;
	//}
}
