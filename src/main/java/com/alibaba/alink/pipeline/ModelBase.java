package com.alibaba.alink.pipeline;

import org.apache.flink.ml.api.core.Model;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

/**
 * The base class for a machine learning model.
 *
 * @param <M> The class type of the {@link ModelBase} implementation itself
 */
public abstract class ModelBase<M extends ModelBase<M>> extends TransformerBase<M>
    implements Model<M> {

    protected Table modelData;

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
    public Table getModelData() {
        return this.modelData;
    }

    /**
     * Set the model data using the Table.
     *
     * @param modelData the Table.
     * @return {@link ModelBase} itself
     */
    public M setModelData(Table modelData) {
        this.modelData = modelData;
        return (M) this;
    }

    @Override
    public M clone() throws CloneNotSupportedException {
        return (M) super.clone().setModelData(this.modelData);
    }
}
