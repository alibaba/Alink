package com.alibaba.alink.operator.common.feature.pca;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
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

    private PcaModelData model = null;

    private int[] featureIdxs = null;
    private boolean isVector;

    private String transformType = null;
    private String pcaType = null;

    private double[] sourceMean = null;
    private double[] sourceStd = null;
    private double[] scoreStd = null;

    private OutputColsHelper outputColsHelper;

    public PcaModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
        super(modelSchema, dataSchema, params);

        transformType = this.params.get(PcaPredictParams.TRANSFORM_TYPE);

        String[] keepColNames = this.params.get(PcaPredictParams.RESERVED_COLS);
        String predResultColName = this.params.get(PcaPredictParams.PREDICTION_COL);
        this.outputColsHelper = new OutputColsHelper(dataSchema, predResultColName, Types.STRING(), keepColNames);
    }

    private int[] checkGetColIndices(Boolean isVector, String[] featureColNames, String vectorColName) {
        String[] colNames = getDataSchema().getFieldNames();
        if (!isVector) {
            TableUtil.assertSelectedColExist(colNames, featureColNames);
            TableUtil.assertNumericalCols(getDataSchema(), featureColNames);
            return TableUtil.findColIndices(colNames, featureColNames);
        } else {
            TableUtil.assertSelectedColExist(colNames, vectorColName);
            TableUtil.assertVectorCols(getDataSchema(), vectorColName);
            return new int[]{TableUtil.findColIndex(colNames, vectorColName)};
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
        int p = model.p;

        PcaTransformTypeEnum transformTypeEnum = PcaTransformTypeEnum.valueOf(this.transformType.toUpperCase());
        PcaTypeEnum pcaTypeEnum = PcaTypeEnum.valueOf(this.pcaType.toUpperCase());

        //transform mean, stdDevs and scoreStd
        sourceMean = new double[nx];
        sourceStd = new double[nx];
        scoreStd = new double[nx];

        Arrays.fill(sourceStd, 1);
        Arrays.fill(scoreStd, 1);

        if (PcaTypeEnum.CORR.equals(pcaTypeEnum)) {
            sourceStd = model.stddevs;
        }

        switch (transformTypeEnum) {
            case SUBMEAN:
                sourceMean = model.means;
                break;
            case NORMALIZATION:
                sourceMean = model.means;
                for (int i = 0; i < p; i++) {
                    double tmp = 0;
                    for (int j = 0; j < nx; j++) {
                        for (int k = 0; k < nx; k++) {
                            tmp += model.coef[i][j] * model.coef[i][k] * model.cov[j][k];
                        }
                    }
                    scoreStd[i] = Math.sqrt(tmp);
                }
                break;
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
        for (int i = 0; i < predictData.length; i++) {
            predictData[i] /= this.scoreStd[i];
        }

        return outputColsHelper.getResultRow(in, Row.of(VectorUtil.toString(new DenseVector(predictData))));
    }


}
