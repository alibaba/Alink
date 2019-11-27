package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.dataproc.SrtPredictMapperParams;

import java.util.List;

/**
 * This mapper changes a row values to a specific range [min, max], the rescaled value is
 * <blockquote>
 *   Rescaled(value) = \frac{value - E_{min}}{E_{max} - E_{min}} * (max - min) + min
 * </blockquote>
 *
 * For the case \(E_{max} == E_{min}\), \(Rescaled(value) = 0.5 * (max + min)\).
 */
public class MinMaxScalerModelMapper extends ModelMapper {
    private int[] selectedColIndices;
    private double[] eMaxs;
    private double[] eMins;
    private double max;
    private double min;

    private OutputColsHelper predictResultColsHelper;

    /**
     * Constructor
     * @param modelSchema the model schema.
     * @param dataSchema  the data schema.
     * @param params      the params.
     */
    public MinMaxScalerModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
        super(modelSchema, dataSchema, params);
        String[] selectedColNames = ImputerModelDataConverter.extractSelectedColNames(modelSchema);
        TypeInformation[] selectedColTypes = ImputerModelDataConverter.extractSelectedColTypes(modelSchema);
        this.selectedColIndices = TableUtil.findColIndices(dataSchema, selectedColNames);

        String[] outputColNames = params.get(SrtPredictMapperParams.OUTPUT_COLS);
        if (outputColNames == null) {
            outputColNames = selectedColNames;
        }

        this.predictResultColsHelper = new OutputColsHelper(dataSchema, outputColNames, selectedColTypes, null);
    }

    /**
     * Load model from the list of Row type data.
     *
     * @param modelRows the list of Row type data.
     */
    @Override
    public void loadModel(List<Row> modelRows) {
        MinMaxScalerModelDataConverter converter = new MinMaxScalerModelDataConverter();
        Tuple4<Double, Double, double[], double[]> tuple4 = converter.load(modelRows);

        this.min = tuple4.f0;
        this.max = tuple4.f1;
        this.eMins = tuple4.f2;
        this.eMaxs = tuple4.f3;
    }

    /**
     * Get the table schema(includs column names and types) of the calculation result.
     *
     * @return the table schema of output Row type data.
     */
    @Override
    public TableSchema getOutputSchema() {
        return this.predictResultColsHelper.getResultSchema();
    }

    /**
     * map operation method.
     *
     * @param row the input Row type data.
     * @return one Row type data.
     * @throws Exception This method may throw exceptions. Throwing
     *                   an exception will cause the operation to fail.
     */
    @Override
    public Row map(Row row) throws Exception {
        if (null == row) {
            return null;
        }
        Row r = new Row(selectedColIndices.length);
        for (int i = 0; i < this.selectedColIndices.length; i++) {
            Object obj = row.getField(this.selectedColIndices[i]);
            if (null != obj) {
                double d;
                if (obj instanceof Number) {
                    d = ((Number) obj).doubleValue();
                } else {
                    d = Double.parseDouble(obj.toString());
                }
                d = ScalerUtil.minMaxScaler(d, eMins[i], eMaxs[i], max, min);
                r.setField(i, d);
            }
        }
        return this.predictResultColsHelper.getResultRow(row, r);
    }
}
