package com.alibaba.alink.operator.common.fm;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;
import com.alibaba.alink.params.recommendation.FmTrainParams;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

/**
 * Fm regressor model train info.
 */
public class FmRegressorModelTrainInfo {

    protected String[] convInfo;
    protected Params meta;
    protected String[] keys;

    public FmRegressorModelTrainInfo(List<Row> rows) {
        DecimalFormat df = new DecimalFormat("#0.00000000");
        for (Row r : rows) {
            if ((int)r.getField(0) == 0) {
                this.meta = JsonConverter.fromJson((String)r.getField(1), Params.class);
            } else if ((int)r.getField(0) == 1) {
                double[] cinfo = JsonConverter.fromJson((String)r.getField(1), double[].class);
                int size = cinfo.length / 3;
                this.convInfo = new String[size];
                setKeys();
                for (int i = 0; i < size; ++i) {
                    this.convInfo[i] = "step: " + i + " loss: " + df.format(cinfo[3 * i])
                        + keys[0] + df.format(cinfo[3 * i + 1]) + keys[1] + df.format(cinfo[3 * i + 2]);
                }
            }
        }
    }

    protected void setKeys() {
        keys = new String[] {" mae: ", " mse: "};
    }

    public String[] getConvInfo() {
        return convInfo;
    }

    public Params getMeta() {
        return meta;
    }

    @Override
    public String toString() {
        StringBuilder sbd = new StringBuilder();

        sbd.append(PrettyDisplayUtils.displayHeadline("train meta info", '-'));
        Map<String, String> map = new HashMap<>();
        map.put("numFeature", meta.get(ModelParamName.VECTOR_SIZE).toString());
        map.put("numFactor", meta.get(FmTrainParams.NUM_FACTOR).toString());
        map.put("hasLinearItem", meta.get(FmTrainParams.WITH_LINEAR_ITEM).toString());
        map.put("hasIntercept", meta.get(FmTrainParams.WITH_INTERCEPT).toString());
        sbd.append(PrettyDisplayUtils.displayMap(map, 2, false) + "\n");

        sbd.append(PrettyDisplayUtils.displayHeadline("train convergence info", '-'));
        if (convInfo.length < 6) {
            for (int i = 0; i < convInfo.length; ++i) {
                sbd.append("" + convInfo[i] + "\n");
            }
        } else {
            for (int i = 0; i < 3; ++i) {
                sbd.append("" + convInfo[i] + "\n");
            }
            sbd.append("" + "... ... ... ..." + "\n");
            for (int i = convInfo.length - 3; i < convInfo.length; ++i) {
                sbd.append("" + convInfo[i] + "\n");
            }
        }

        return sbd.toString();
    }
}
