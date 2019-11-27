package com.alibaba.alink.pipeline.dataproc;

import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelMapper;
import com.alibaba.alink.params.dataproc.MultiStringIndexerPredictParams;
import com.alibaba.alink.pipeline.MapModel;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Model fitted by {@link MultiStringIndexer}.
 */
public class MultiStringIndexerModel extends MapModel<MultiStringIndexerModel>
    implements MultiStringIndexerPredictParams<MultiStringIndexerModel> {

    public MultiStringIndexerModel(Params params) {
        super(MultiStringIndexerModelMapper::new, params);
    }
}