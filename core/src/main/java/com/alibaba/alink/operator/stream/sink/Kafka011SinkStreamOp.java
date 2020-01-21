package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.operator.common.io.kafka.Kafka011OutputFormat;
import com.alibaba.alink.operator.common.io.kafka.SimpleKafkaConverter;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.Kafka011SinkParams;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;

import static com.alibaba.alink.common.utils.JsonConverter.gson;


@IoOpAnnotation(name = "kafka011", ioType = IOType.SinkStream)
public final class Kafka011SinkStreamOp extends BaseSinkStreamOp<Kafka011SinkStreamOp>
    implements Kafka011SinkParams<Kafka011SinkStreamOp> {


    public Kafka011SinkStreamOp() {
        this(new Params());
    }

    public Kafka011SinkStreamOp(Params params) {
        super(AnnotationUtils.annotatedName(Kafka011SinkStreamOp.class), params);
    }

    @Override
    public Kafka011SinkStreamOp sinkFrom(StreamOperator in) {
        final String dataFormat = getDataFormat();
        final String fieldDelimiter = CsvUtil.unEscape(getFieldDelimiter());
        final String[] colNames = in.getColNames();

        DataStream<Row> outputRows = in.getDataStream();
        DataStream<Row> serialized = outputRows
            .map(new RichMapFunction<Row, byte[]>() {
                transient RowSerializer rowSerializer;

                @Override
                public void open(Configuration parameters) throws Exception {
                    if (dataFormat.equalsIgnoreCase("csv")) {
                        this.rowSerializer = new CsvSerializer(fieldDelimiter);
                    } else if (dataFormat.equalsIgnoreCase("json")) {
                        this.rowSerializer = new JsonSerializer(colNames);
                    } else {
                        throw new IllegalArgumentException("unknown data format: " + dataFormat);
                    }
                }

                @Override
                public byte[] map(Row value) throws Exception {
                    return this.rowSerializer.serialize(value);
                }
            })
            .map(new MapFunction<byte[], Row>() {
                @Override
                public Row map(byte[] value) throws Exception {
                    return Row.of(value);
                }
            });
        DataStream<Tuple2<Boolean, Row>> richOutputRows = serialized
            .map(new MapFunction<Row, Tuple2<Boolean, Row>>() {
                @Override
                public Tuple2<Boolean, Row> map(Row value) throws Exception {
                    return Tuple2.of(true, value);
                }
            });
        richOutputRows.writeUsingOutputFormat(createOutputFormat()).name("kafka011_" + getTopic());
        return this;
    }

    private OutputFormat<Tuple2<Boolean, Row>> createOutputFormat() {
        final String topic = getTopic();
        final String boostrapServers = getBootstrapServers();
        final String clientId = "kafka_011_" + new Random().nextLong();

        Properties props = new Properties();
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        SimpleKafkaConverter kafkaConverter = new SimpleKafkaConverter();

        Kafka011OutputFormat.Builder builder = new Kafka011OutputFormat.Builder();
        builder.setTopic(topic)
            .setKafkaConverter(kafkaConverter)
            .setProperties(props);
        return builder.build();
    }

    /**
     * Serialize a row to byte array.
     */
    public interface RowSerializer extends Serializable {
        public byte[] serialize(Row row) throws Exception;
    }

    /**
     * Serialize a row to JSON format.
     */
    public static class JsonSerializer implements RowSerializer {
        String[] colNames;

        public JsonSerializer(String[] colNames) {
            this.colNames = colNames;
        }

        public byte[] serialize(Row row) throws Exception {
            HashMap<String, Object> map = new HashMap<>();
            for (int i = 0; i < colNames.length; i++) {
                Object obj = row.getField(i);
                if (obj != null) {
                    map.put(colNames[i], obj);
                }
            }
            String str = gson.toJson(map);
            return str.getBytes("UTF-8");
        }
    }

    /**
     * Serialize a row to CSV format.
     */
    public static class CsvSerializer implements RowSerializer {
        String fieldDelimiter;

        public CsvSerializer(String fieldDelimiter) {
            this.fieldDelimiter = fieldDelimiter;
        }

        public byte[] serialize(Row row) throws Exception {
            StringBuilder sbd = new StringBuilder();
            int n = row.getArity();
            for (int i = 0; i < n; i++) {
                Object obj = row.getField(i);
                if (obj != null) {
                    sbd.append(obj);
                }
                if (i != n - 1) {
                    sbd.append(this.fieldDelimiter);
                }
            }
            String str = sbd.toString();
            return str.getBytes("UTF-8");
        }
    }
}

