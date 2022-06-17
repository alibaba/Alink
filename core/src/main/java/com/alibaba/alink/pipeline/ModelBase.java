package com.alibaba.alink.pipeline;

import org.apache.flink.ml.api.core.Model;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.params.io.ModelFileSinkParams;
import com.alibaba.alink.params.shared.HasModelFilePath;

/**
 * The base class for a machine learning model.
 *
 * @param <M> The class type of the {@link ModelBase} implementation itself
 */
public abstract class ModelBase<M extends ModelBase <M>> extends TransformerBase <M>
	implements Model <M>, ModelFileSinkParams <M> {

	private static final long serialVersionUID = 1181492490109006467L;
	protected BatchOperator <?> modelData;

	public ModelBase() {
		super();
	}

	public ModelBase(Params params) {
		super(params);
	}

	/**
	 * Get model data as Table representation.
	 *
	 * @return the Table
	 */
	public BatchOperator <?> getModelData() {
		if (this.modelData != null) {
			return this.modelData;
		} else if (getParams().get(HasModelFilePath.MODEL_FILE_PATH) != null) {
			return new AkSourceBatchOp()
				.setFilePath(getModelFilePath())
				.setMLEnvironmentId(getMLEnvironmentId());
		} else {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Set the model data using the Table.
	 *
	 * @param modelData the Table.
	 * @return {@link ModelBase} itself
	 */
	public M setModelData(BatchOperator <?> modelData) {
		this.modelData = modelData;
		return (M) this;
	}

	@Override
	public M clone() throws CloneNotSupportedException {
		return (M) super.clone().setModelData(this.modelData);
	}
}
