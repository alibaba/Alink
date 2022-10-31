package com.alibaba.alink.pipeline;

import org.apache.flink.ml.api.core.Model;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.AkSourceLocalOp;
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
	protected LocalOperator <?> modelData_local;
	protected ModelFileData modelFileData;

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
		if (null != this.modelData) {
			return this.modelData;
		} else if (null != this.modelData_local) {
			return new MemSourceBatchOp(modelData_local.getOutputTable())
				.setMLEnvironmentId(getMLEnvironmentId());
		} else if (null != this.modelFileData) {
			return this.modelFileData.getBatchData(getMLEnvironmentId());
		} else if (getParams().get(HasModelFilePath.MODEL_FILE_PATH) != null) {
			return new AkSourceBatchOp()
				.setFilePath(getModelFilePath())
				.setMLEnvironmentId(getMLEnvironmentId());
		} else {
			throw new AkIllegalDataException("Failed to get model data. ");
		}
	}

	public LocalOperator <?> getModelDataLocal() {
		if (null!= this.modelData_local ) {
			return this.modelData_local;
		} else if (null != this.modelFileData) {
			return this.modelFileData.getLocalData();
		} else if (getParams().get(HasModelFilePath.MODEL_FILE_PATH) != null) {
			return new AkSourceLocalOp()
				.setFilePath(getModelFilePath());
		} else {
			throw new AkIllegalDataException("Failed to get model data. ");
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

	public M setModelData(LocalOperator <?> modelData) {
		this.modelData_local = modelData;
		return (M) this;
	}

	@Override
	public M clone() throws CloneNotSupportedException {
		return (M) super.clone().setModelData(this.modelData);
	}
}
