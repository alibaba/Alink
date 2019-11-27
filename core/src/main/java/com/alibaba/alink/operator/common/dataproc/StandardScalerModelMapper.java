package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.dataproc.SrtPredictMapperParams;

import java.util.List;

public class StandardScalerModelMapper extends ModelMapper {
    private String[] selectedColNames;
    private TypeInformation[] selectedColTypes;
    private int[] selectedColIndices;
    private double[] means;
    private double[] stddevs;

    private OutputColsHelper predResultColsHelper;

    public StandardScalerModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
        super(modelSchema, dataSchema, params);
        this.selectedColNames = ImputerModelDataConverter.extractSelectedColNames(modelSchema);
        this.selectedColTypes = ImputerModelDataConverter.extractSelectedColTypes(modelSchema);
        this.selectedColIndices = TableUtil.findColIndices(dataSchema, selectedColNames);

        String[] outputColNames = params.get(SrtPredictMapperParams.OUTPUT_COLS);
        if (outputColNames == null) {
            outputColNames = selectedColNames;
        }

        this.predResultColsHelper = new OutputColsHelper(dataSchema,
            outputColNames,
            this.selectedColTypes,
            null);
    }

    /**
     * Load model from the list of Row type data.
     *
     * @param modelRows the list of Row type data
     */
    @Override
    public void loadModel(List<Row> modelRows) {
        StandardScalerModelDataConverter converter = new StandardScalerModelDataConverter();
        Tuple2<double[], double[]> tuple2 = converter.load(modelRows);

        means = tuple2.f0;
        stddevs = tuple2.f1;
    }

    /**
     * Get the table schema(includs column names and types) of the calculation result.
     *
     * @return the table schema of output Row type data
     */
    @Override
    public TableSchema getOutputSchema() {
        return this.predResultColsHelper.getResultSchema();
    }

    /**
     * map operation method.
     *
     * @param row the input Row type data
     * @return one Row type data
     * @throws Exception This method may throw exceptions. Throwing
     *                   an exception will cause the operation to fail.
     */
    @Override
    public Row map(Row row) throws Exception {
        Row r = new Row(this.selectedColIndices.length);
        for (int i = 0; i < this.selectedColIndices.length; i++) {
            Object obj = row.getField(this.selectedColIndices[i]);
            if (null != obj) {
                if (this.stddevs[i] > 0) {
                    double d = (((Number) obj).doubleValue() - this.means[i]) / this.stddevs[i];
                    r.setField(i, d);
                } else {
                    r.setField(i, 0.0);
                }
            }
        }
        return this.predResultColsHelper.getResultRow(row, r);
    }
}
