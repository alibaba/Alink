package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.feature.QuantileDiscretizerPredictParams;
import com.alibaba.alink.params.shared.colname.HasOutputColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

import java.util.Arrays;
import java.util.List;
import java.util.Map;


/**
 * quantile discretizer model data mapper.
 */
public class QuantileDiscretizerModelMapper extends ModelMapper {
    QuantileDiscretizerModelDataConverter model;

    int[] idx;
    double[][] bounds;
    OutputColsHelper outputColsHelper;

    public QuantileDiscretizerModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
        super(modelSchema, dataSchema, params);

        String[] input = params.get(HasSelectedCols.SELECTED_COLS);
        String[] output = params.get(HasOutputColsDefaultAsNull.OUTPUT_COLS);

        if (output == null) {
            output = input;
        } else {
            if (input.length != output.length) {
                throw new RuntimeException(
                    "Input column name is not match output column name. input: " + JsonConverter.gson.toJson(input)
                        + ", output: " + JsonConverter.gson.toJson(output));
            }
        }

        outputColsHelper = new OutputColsHelper(
            dataSchema, output,
            Arrays.stream(output).map(x -> Types.LONG).toArray(TypeInformation[]::new),
            this.params.get(QuantileDiscretizerPredictParams.RESERVED_COLS));
    }

    @Override
    public void loadModel(List<Row> modelRows) {
        model = new QuantileDiscretizerModelDataConverter();
        model.load(modelRows);

        Map<String, double[]> data = model.getData();
        String[] inputColNames = params.get(HasSelectedCols.SELECTED_COLS);

        idx = new int[inputColNames.length];
        bounds = new double[inputColNames.length][];
        for (int i = 0; i < inputColNames.length; ++i) {
            if (data.containsKey(inputColNames[i])) {
                idx[i] = TableUtil.findColIndex(getDataSchema().getFieldNames(), inputColNames[i]);
                double[] bound = data.get(inputColNames[i]);
                bounds[i] = data.get(inputColNames[i]);

                int len = 2;
                if (bound != null) {
                    len += bound.length;
                }

                bounds[i] = new double[len];
                bounds[i][0] = Double.NEGATIVE_INFINITY;
                bounds[i][len - 1] = Double.POSITIVE_INFINITY;

                if (bound != null) {
                    System.arraycopy(bound, 0, bounds[i], 1, bound.length);
                }
            } else {
                throw new RuntimeException("Model has not " + inputColNames[i] + ", which is in inputColNames");
            }
        }
    }

    @Override
    public TableSchema getOutputSchema() {
        return outputColsHelper.getResultSchema();
    }

    @Override
    public Row map(Row row) throws Exception {
        Row out = new Row(idx.length);

        for (int i = 0; i < idx.length; ++i) {
            if (row.getField(idx[i]) == null) {
                continue;
            }

            double val = ((Number) row.getField(idx[i])).doubleValue();

            if (Double.isNaN(val)) {
                continue;
            }

            if (bounds[i] == null) {
                continue;
            }

            int hit = Arrays.binarySearch(bounds[i], val);

            hit = hit >= 0 ? hit - 1 : -hit - 2;

            out.setField(i, Long.valueOf(hit));
        }

        return outputColsHelper.getResultRow(row, out);
    }
}
