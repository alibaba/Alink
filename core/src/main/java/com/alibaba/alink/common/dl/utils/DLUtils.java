package com.alibaba.alink.common.dl.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.dl.coding.ExampleCodingConfigV2;
import com.alibaba.alink.common.dl.coding.ExampleCodingV2;
import com.alibaba.alink.common.dl.data.DataTypesV2;
import com.alibaba.alink.common.dl.data.TFRecordReaderImpl;
import com.alibaba.alink.common.dl.data.TFRecordWriterImpl;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.tensorflow2.client.DLConfig;
import com.alibaba.flink.ml.tensorflow2.util.TFConstants;
import com.alibaba.flink.ml.util.MLConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class DLUtils implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DLUtils.class);

    private static final Map<TypeInformation<?>, DataTypesV2> TYPE_INFO_TO_DATA_TYPE = new HashMap <>();

    static {
        TYPE_INFO_TO_DATA_TYPE.put(AlinkTypes.STRING, DataTypesV2.STRING);
        TYPE_INFO_TO_DATA_TYPE.put(AlinkTypes.FLOAT, DataTypesV2.FLOAT_32);
        TYPE_INFO_TO_DATA_TYPE.put(AlinkTypes.DOUBLE, DataTypesV2.FLOAT_64);
        TYPE_INFO_TO_DATA_TYPE.put(AlinkTypes.INT, DataTypesV2.INT_32);
        TYPE_INFO_TO_DATA_TYPE.put(AlinkTypes.LONG, DataTypesV2.INT_64);
        TYPE_INFO_TO_DATA_TYPE.put(AlinkTypes.SHORT, DataTypesV2.INT_16);
        TYPE_INFO_TO_DATA_TYPE.put(AlinkTypes.BYTE, DataTypesV2.INT_8);
        TYPE_INFO_TO_DATA_TYPE.put(AlinkTypes.BOOLEAN, DataTypesV2.BOOL);

        TYPE_INFO_TO_DATA_TYPE.put(AlinkTypes.TENSOR, DataTypesV2.TENSOR);
        TYPE_INFO_TO_DATA_TYPE.put(AlinkTypes.FLOAT_TENSOR, DataTypesV2.FLOAT_TENSOR);
        TYPE_INFO_TO_DATA_TYPE.put(AlinkTypes.DOUBLE_TENSOR, DataTypesV2.DOUBLE_TENSOR);
        TYPE_INFO_TO_DATA_TYPE.put(AlinkTypes.INT_TENSOR, DataTypesV2.INT_TENSOR);
        TYPE_INFO_TO_DATA_TYPE.put(AlinkTypes.LONG_TENSOR, DataTypesV2.LONG_TENSOR);
        TYPE_INFO_TO_DATA_TYPE.put(AlinkTypes.BYTE_TENSOR, DataTypesV2.BYTE_TENSOR);
        TYPE_INFO_TO_DATA_TYPE.put(AlinkTypes.UBYTE_TENSOR, DataTypesV2.UBYTE_TENSOR);
        TYPE_INFO_TO_DATA_TYPE.put(AlinkTypes.BOOL_TENSOR, DataTypesV2.BOOLEAN_TENSOR);
        TYPE_INFO_TO_DATA_TYPE.put(AlinkTypes.STRING_TENSOR, DataTypesV2.STRING_TENSOR);
    }

    public static void safePutProperties(MLContext mlContext, String k, String v) {
        safePutProperties(mlContext.getProperties(), k, v);

    }

    public static void safePutProperties(DLConfig DLConfig, String k, String v) {
        safePutProperties(DLConfig.getProperties(), k, v);

    }

    private static void safePutProperties(Map<String, String> properties, String k, String v) {
		AkPreconditions.checkNotNull(v, String.format("Null value encountered for key %s.", k));
		properties.put(k, v);
    }

    private static DataTypesV2 toFlinkAiExtendTypes(TypeInformation<?> typeInfo) {
        DataTypesV2 type = TYPE_INFO_TO_DATA_TYPE.get(typeInfo);
        if (type == null) {
            throw new AkUnsupportedOperationException("Not supported type: " + typeInfo);
        }
        return type;
    }

    public static void setExampleCodingType(DLConfig config, TableSchema inputSchema, TableSchema outputSchema) {
        if (inputSchema != null) {
            String[] names = inputSchema.getFieldNames();
            DataTypesV2[] types = new DataTypesV2[names.length];
            for (int i = 0; i < types.length; i++) {
                types[i] = toFlinkAiExtendTypes(inputSchema.getFieldTypes()[i]);
            }
            String str = ExampleCodingConfigV2.createExampleConfigStr(names, types,
                ExampleCodingConfigV2.ObjectType.ROW, Row.class);
            DLUtils.safePutProperties(config, TFConstants.INPUT_TF_EXAMPLE_CONFIG, str);
            LOG.info("InputExampleConfigStr: {}", str);
            if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
                System.out.println("InputExampleConfigStr: " + str);
            }
        }

        if (outputSchema != null) {
            String[] namesOutput = outputSchema.getFieldNames();
            DataTypesV2[] typesOutput = new DataTypesV2[namesOutput.length];
            for (int i = 0; i < typesOutput.length; i++) {
                typesOutput[i] = toFlinkAiExtendTypes(outputSchema.getFieldTypes()[i]);
            }
            String strOutput = ExampleCodingConfigV2.createExampleConfigStr(namesOutput, typesOutput,
                ExampleCodingConfigV2.ObjectType.ROW, Row.class);
            DLUtils.safePutProperties(config, TFConstants.OUTPUT_TF_EXAMPLE_CONFIG, strOutput);
            LOG.info("OutputExampleConfigStr: {}", strOutput);
            if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
                System.out.println("OutputExampleConfigStr: " + strOutput);
            }
        }

        DLUtils.safePutProperties(config, MLConstants.ENCODING_CLASS, ExampleCodingV2.class.getCanonicalName());
        DLUtils.safePutProperties(config, MLConstants.DECODING_CLASS, ExampleCodingV2.class.getCanonicalName());
        DLUtils.safePutProperties(config, MLConstants.RECORD_READER_CLASS, TFRecordReaderImpl.class.getCanonicalName());
        DLUtils.safePutProperties(config, MLConstants.RECORD_WRITER_CLASS, TFRecordWriterImpl.class.getCanonicalName());
    }

    /**
     * Transform string value to byte array with UTF-8 encoding.
     * This is a temporary workaround to the problem that Flink-ai-extend uses
     * ISO_8859_1 encoding.
     */
    public static Row encodeStringValue(Row row) {
        Row encoded = new Row(row.getArity());
        for (int i = 0; i < row.getArity(); i++) {
            Object v = row.getField(i);
            if (v instanceof String) {
                encoded.setField(i, ((String) v).getBytes(Charset.forName("UTF-8")));
            } else {
                encoded.setField(i, v);
            }
        }
        return encoded;
    }
}
