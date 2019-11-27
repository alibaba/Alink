package com.alibaba.alink.pipeline.dataproc;

import com.alibaba.alink.operator.common.dataproc.IndexToStringModelMapper;
import com.alibaba.alink.params.dataproc.IndexToStringPredictParams;
import com.alibaba.alink.pipeline.MapModel;
import com.alibaba.alink.pipeline.Pipeline;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

/**
 * Maps columns of indices to strings, based on the model fitted by {@link StringIndexer}.
 * <p>
 * While {@link StringIndexerModel} maps string to index, IndexToString maps index to string.
 * However, IndexToString does not have a corresponding {@link com.alibaba.alink.pipeline.EstimatorBase}.
 * Instead, IndexToString uses model data in StringIndexerModel to perform predictions.
 * <p>
 * <p>IndexToString use the name of the {@link StringIndexerModel} to get the model data.
 * The referenced {@link StringIndexerModel} should be created before calling <code>transform</code> method.
 * <p>
 * A common use case is as follows:
 * <p>
 * <code>
 * StringIndexer stringIndexer = new StringIndexer()
 * .setModelName("name_a") // The fitted StringIndexerModel will have name "name_a".
 * .setSelectedCol(...);
 * <p>
 * StringIndexerModel model = stringIndexer.fit(...); // This model will have name "name_a".
 * <p>
 * IndexToString indexToString = new IndexToString()
 * .setModelName("name_a") // Should match the name of one StringIndexerModel.
 * .setSelectedCol(...)
 * .setOutputCol(...);
 * <p>
 * indexToString.transform(...); // Will relies on a StringIndexerModel with name "name_a" to do transformation.
 * </code>
 * <p>
 * The reason we use model name registration mechanism here is to make possible stacking both StringIndexer and
 * IndexToString into a {@link Pipeline}. For examples,
 * <p>
 * <code>
 * StringIndexer stringIndexer = new StringIndexer()
 * .setModelName("si_model_0").setSelectedCol("label");
 * <p>
 * MultilayerPerceptronClassifier mlpc = new MultilayerPerceptronClassifier()
 * .setVectorCol("features").setLabelCol("label").setPredictionCol("predicted_label");
 * <p>
 * IndexToString indexToString = new IndexToString()
 * .setModelName("si_model_0").setSelectedCol("predicted_label");
 * <p>
 * Pipeline pipeline = new Pipeline().add(stringIndexer).add(mlpc).add(indexToString);
 * <p>
 * pipeline.fit(...);
 * </code>
 */
public class IndexToString extends MapModel<IndexToString>
    implements IndexToStringPredictParams<IndexToString> {

    public IndexToString() {
        this(new Params());
    }

    public IndexToString(Params params) {
        super(IndexToStringModelMapper::new, params);
    }

    @Override
    public Table getModelData() {
        if (this.modelData == null) {
            this.setModelData(StringIndexerModel.getRegisteredModel(super.params.get(StringIndexer.MODEL_NAME))
                .getModelData());
        }
        return this.modelData;
    }
}