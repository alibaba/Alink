package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.params.recommendation.BaseRecommParams;

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
	protected TypeInformation<?> recommObjType;
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
			= new OutputColsHelper(dataSchema, resultColName, AlinkTypes.DOUBLE, reservedColNames);
		this.outputColsHelper4RecommObjs
			= new OutputColsHelper(dataSchema, resultColName, AlinkTypes.DOUBLE, reservedColNames);
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
				throw new AkUnsupportedOperationException("NOT supported recommend type : " + recommType);
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
	abstract MTable recommendItemsPerUser(Object infoUser) throws Exception;

	/**
	 * Recommend users observed for the input item.
	 *
	 * @param infoItem The input item info.
	 * @return Json string of the result values.
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation to fail.
	 */
	abstract MTable recommendUsersPerItem(Object infoItem) throws Exception;

	/**
	 * Recommend the k most similar items for the input item.
	 *
	 * @param infoItem The input item info.
	 * @return Json string of the result values.
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation to fail.
	 */
	abstract MTable recommendSimilarItems(Object infoItem) throws Exception;

	/**
	 * Recommend the k most similar users for the input user.
	 *
	 * @param infoUser The input user info.
	 * @return Json string of the result values.
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation to fail.
	 */
	abstract MTable recommendSimilarUsers(Object infoUser) throws Exception;

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

	public abstract RecommKernel createNew();
}
