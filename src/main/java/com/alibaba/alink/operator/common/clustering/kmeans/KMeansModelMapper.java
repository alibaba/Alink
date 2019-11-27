package com.alibaba.alink.operator.common.clustering.kmeans;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;
import com.alibaba.alink.params.clustering.KMeansPredictParams;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.*;

/**
 * Find  the closest cluster center for every point.
 */
public class KMeansModelMapper extends ModelMapper {
    private KMeansPredictModelData modelData;
    private int[] colIdx;
    private transient DenseMatrix distanceMatrix;
    private FastDistance distance;
    private final OutputColsHelper outputColsHelper;
    private final boolean isPredDetail;
    private final boolean isPredDistance;

    public KMeansModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
        super(modelSchema, dataSchema, params);
        String[] reservedColNames = this.params.get(KMeansPredictParams.RESERVED_COLS);
        String predResultColName = this.params.get(KMeansPredictParams.PREDICTION_COL);
        isPredDetail = params.contains(KMeansPredictParams.PREDICTION_DETAIL_COL);
        isPredDistance = params.contains(KMeansPredictParams.PREDICTION_DISTANCE_COL);
        List<String> outputCols = new ArrayList<>();
        List<TypeInformation> outputTypes = new ArrayList<>();
        outputCols.add(predResultColName);
        outputTypes.add(Types.LONG);
        if (isPredDetail) {
            outputCols.add(params.get(KMeansPredictParams.PREDICTION_DETAIL_COL));
            outputTypes.add(Types.STRING);
        }
        if (isPredDistance) {
            outputCols.add(params.get(KMeansPredictParams.PREDICTION_DISTANCE_COL));
            outputTypes.add(Types.DOUBLE);
        }
        this.outputColsHelper = new OutputColsHelper(dataSchema, outputCols.toArray(new String[0]),
            outputTypes.toArray(new TypeInformation[0]), reservedColNames);
    }

    @Override
    public TableSchema getOutputSchema() {
        return outputColsHelper.getResultSchema();
    }

    @Override
    public Row map(Row row){
        Vector record = KMeansUtil.getKMeansPredictVector(colIdx, row);

        List<Object> res = new ArrayList<>();
        if (null == record) {
            res.add(null);
            if(isPredDetail){
                res.add(null);
            }
            if(isPredDistance){
                res.add(null);
            }
        }else{
            FastDistanceVectorData vectorData = distance.prepareVectorData(Tuple2.of(record, null));
            double[] clusterDistances = KMeansUtil.getClusterDistances(
                vectorData,
                this.modelData.centroids,
                distance,
                distanceMatrix);
            int index = KMeansUtil.getMinPointIndex(clusterDistances, this.modelData.params.k);
            res.add((long)index);
            if(isPredDetail){
                double[] probs = KMeansUtil.getProbArrayFromDistanceArray(clusterDistances);
                DenseVector vec = new DenseVector(probs.length);
                for(int i = 0; i < this.modelData.params.k; i++){
                    vec.set((int)this.modelData.getClusterId(i), probs[i]);
                }
                res.add(vec.toString());
            }
            if(isPredDistance){
                res.add(clusterDistances[index]);
            }
        }

        return outputColsHelper.getResultRow(row, Row.of(res.toArray(new Object[0])));
    }

    @Override
    public void loadModel(List<Row> modelRows) {
        this.modelData = new KMeansModelDataConverter().load(modelRows);
        this.distance = (FastDistance)this.modelData.params.distanceType.getContinuousDistance();
        this.distanceMatrix = new DenseMatrix(this.modelData.params.k, 1);
        this.colIdx = KMeansUtil.getKmeansPredictColIdxs(this.modelData.params, getDataSchema().getFieldNames());
    }
}
