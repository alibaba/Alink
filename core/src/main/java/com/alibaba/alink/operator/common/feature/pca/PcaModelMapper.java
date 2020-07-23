package com.alibaba.alink.operator.common.feature.pca;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.feature.HasCalculationType;
import com.alibaba.alink.params.feature.PcaPredictParams;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;

/**
 * Predictor for pca which will be used for stream and batch predict
 */
public class PcaModelMapper extends ModelMapper {

    private static final long serialVersionUID = -6656670267982283314L;
    private PcaModelData model = null;

    private int[] featureIdxs = null;
    private boolean isVector;

    private HasCalculationType.CalculationType pcaType = null;

    private double[] sourceMean = null;
    private double[] sourceStd = null;

    private OutputColsHelper outputColsHelper;

    public PcaModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
        super(modelSchema, dataSchema, params);

        String[] keepColNames = this.params.get(PcaPredictParams.RESERVED_COLS);
        String predResultColName = this.params.get(PcaPredictParams.PREDICTION_COL);
        this.outputColsHelper = new OutputColsHelper(dataSchema, predResultColName, Types.STRING(), keepColNames);
    }

    private int[] checkGetColIndices(Boolean isVector, String[] featureColNames, String vectorColName) {
        String[] colNames = getDataSchema().getFieldNames();
        if (!isVector) {
            TableUtil.assertSelectedColExist(colNames, featureColNames);
            TableUtil.assertNumericalCols(getDataSchema(), featureColNames);
            return TableUtil.findColIndicesWithAssertAndHint(colNames, featureColNames);
        } else {
            TableUtil.assertSelectedColExist(colNames, vectorColName);
            TableUtil.assertVectorCols(getDataSchema(), vectorColName);
            return new int[]{TableUtil.findColIndexWithAssertAndHint(colNames, vectorColName)};
        }
    }

    @Override
    public void loadModel(List<Row> modelRows) {
        model = new PcaModelDataConverter().load(modelRows);

        String[] featureColNames = model.featureColNames;
        String vectorColName = model.vectorColName;

        if (params.contains(PcaPredictParams.VECTOR_COL)) {
            vectorColName = params.get(PcaPredictParams.VECTOR_COL);
        }

        if (vectorColName != null) {
            this.isVector = true;
        }

        this.featureIdxs = checkGetColIndices(isVector, featureColNames, vectorColName);
        this.pcaType = model.pcaType;
        int nx = model.means.length;

        //transform mean, stdDevs and scoreStd
        sourceMean = new double[nx];
        sourceStd = new double[nx];
        Arrays.fill(sourceStd, 1);

        if (HasCalculationType.CalculationType.CORR == this.pcaType) {
            sourceStd = model.stddevs;
            sourceMean = model.means;
        }
    }

    @Override
    public TableSchema getOutputSchema() {
        return this.outputColsHelper.getResultSchema();
    }

    @Override
    public Row map(Row in) throws Exception {
        //transform data
        double[] data = new double[this.model.nx];
        if (isVector) {
            Vector parsed = VectorUtil.getVector(in.getField(featureIdxs[0]));
            if (parsed instanceof SparseVector) {
                if (parsed.size() < 0) {
                    ((SparseVector) parsed).setSize(model.nx);
                }
            }
            for (int i = 0; i < parsed.size(); i++) {
                data[i] = parsed.get(i);
            }
        } else {
            for (int i = 0; i < this.featureIdxs.length; ++i) {
                data[i] = (Double) in.getField(this.featureIdxs[i]);
            }
        }

        double[] predictData;
        if (model.idxNonEqual.length != data.length) {
            Integer[] indices = model.idxNonEqual;
            double[] dataNe = new double[indices.length];
            for (int i = 0; i < indices.length; i++) {
                if (Math.abs(this.sourceStd[i]) > 1e-12) {
                    int idx = indices[i];
                    dataNe[i] = (data[idx] - this.sourceMean[i]) / this.sourceStd[i];
                }
            }
            predictData = model.calcPrinValue(dataNe);
        } else {
            for (int i = 0; i < data.length; i++) {
                if (Math.abs(this.sourceStd[i]) > 1e-12) {
                    data[i] = (data[i] - this.sourceMean[i]) / this.sourceStd[i];
                }
            }
            predictData = model.calcPrinValue(data);
        }

        return outputColsHelper.getResultRow(in, Row.of(VectorUtil.toString(new DenseVector(predictData))));
    }
}
