package com.alibaba.alink.pipeline;

import com.alibaba.alink.params.shared.HasMLEnvironmentId;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.api.misc.param.WithParams;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;

/**
 * The base class for a stage in a pipeline, either an [[EstimatorBase]] or a [[TransformerBase]].
 *
 * <p>The PipelineStageBase maintains the parameters for the stage.
 * A default constructor is needed in order to restore a pipeline stage.
 *
 * @param <S> The class type of the {@link PipelineStageBase} implementation itself, used by {@link
 *            org.apache.flink.ml.api.misc.param.WithParams} and Cloneable.
 */
public abstract class PipelineStageBase<S extends PipelineStageBase<S>>
    implements WithParams<S>, HasMLEnvironmentId<S>, Cloneable {
    protected Params params;

    public PipelineStageBase() {
        this(null);
    }

    public PipelineStageBase(Params params) {
        if (null == params) {
            this.params = new Params();
        } else {
            this.params = params.clone();
        }
    }

    @Override
    public Params getParams() {
        if (null == this.params) {
            this.params = new Params();
        }
        return this.params;
    }

    @Override
    public S clone() throws CloneNotSupportedException {
        PipelineStageBase result = (PipelineStageBase) super.clone();
        result.params = this.params.clone();
        return (S) result;
    }

    protected static TableEnvironment tableEnvOf(Table table) {
        return ((TableImpl) table).getTableEnvironment();
    }
}
