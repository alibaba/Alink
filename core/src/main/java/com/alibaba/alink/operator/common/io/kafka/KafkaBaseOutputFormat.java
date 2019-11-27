/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.alibaba.alink.operator.common.io.kafka;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase;
import org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaMetricWrapper;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static java.util.Objects.requireNonNull;

public abstract class KafkaBaseOutputFormat extends TupleRichOutputFormat {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaProducerBase.class);

    private static final long serialVersionUID = 1L;

    /**
     * Configuration key for disabling the metrics reporting.
     */
    public static final String KEY_DISABLE_METRICS = "flink.disable-metrics";

    /**
     * User defined properties for the Producer.
     */
    protected final Properties producerConfig;

    /**
     * The name of the default topic this producer is writing data to.
     */
    protected final String defaultTopicId;

    /**
     * (Serializable) SerializationSchema for turning objects used with Flink into.
     * byte[] for Kafka.
     */
    protected final KafkaConverter<Row> schema;

    /**
     * Partitions of each topic.
     */
    protected final Map<String, int[]> topicPartitionsMap;

    /**
     * Flag indicating whether to accept failures (and log them), or to fail on failures.
     */
    protected boolean logFailuresOnly;

    /**
     * If true, the producer will wait until all outstanding records have been send to the broker.
     */
    protected boolean flushOnCheckpoint;

    // -------------------------------- Runtime fields ------------------------------------------

    /**
     * KafkaProducer instance.
     */
    protected transient KafkaProducer<byte[], byte[]> producer;

    /**
     * The callback than handles error propagation or logging callbacks.
     */
    protected transient Callback callback;

    /**
     * Errors encountered in the async producer are stored here.
     */
    protected transient volatile Exception asyncException;

    /**
     * Lock for accessing the pending records.
     */
    protected static class SerializableObject implements java.io.Serializable {
        private static final long serialVersionUID = -7322636177391854669L;
    }

    protected final SerializableObject pendingRecordsLock = new SerializableObject();

    /**
     * Number of unacknowledged records.
     */
    protected long pendingRecords;

    public KafkaBaseOutputFormat(
        String defaultTopicId, KafkaConverter serializationSchema, Properties
        producerConfig) {
        requireNonNull(defaultTopicId, "TopicID not set");
        requireNonNull(serializationSchema, "serializationSchema not set");
        requireNonNull(producerConfig, "producerConfig not set");
        ClosureCleaner.ensureSerializable(serializationSchema);

        this.defaultTopicId = defaultTopicId;
        this.schema = serializationSchema;
        this.producerConfig = producerConfig;

        // set the producer configuration properties for kafka record key value serializers.
        if (!producerConfig.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
            this.producerConfig.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getCanonicalName());
        } else {
            LOG.warn("Overwriting the '{}' is not recommended", ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        }

        if (!producerConfig.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
            this.producerConfig.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getCanonicalName());
        } else {
            LOG.warn("Overwriting the '{}' is not recommended", ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        }

        // eagerly ensure that bootstrap servers are set.
        if (!this.producerConfig.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            throw new IllegalArgumentException(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG + " must be supplied in the producer config properties.");
        }

        this.topicPartitionsMap = new HashMap<>();
    }

    // ---------------------------------- Properties --------------------------

    /**
     * Used for testing only.
     */
    @VisibleForTesting
    protected <K, V> KafkaProducer<K, V> getKafkaProducer(Properties props) {
        return new KafkaProducer<>(props);
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        super.open(taskNumber, numTasks);
        producer = getKafkaProducer(this.producerConfig);

        RuntimeContext ctx = getRuntimeContext();
        schema.open(ctx);

        LOG.info("Starting FlinkKafkaProducer ({}/{}) to produce into default topic {}",
            ctx.getIndexOfThisSubtask() + 1, ctx.getNumberOfParallelSubtasks(), defaultTopicId);

        // register Kafka metrics to Flink accumulators
        if (!Boolean.parseBoolean(producerConfig.getProperty(KEY_DISABLE_METRICS, "false"))) {
            Map<MetricName, ? extends Metric> metrics = this.producer.metrics();

            if (metrics == null) {
                // MapR's Kafka implementation returns null here.
                LOG.info("Producer implementation does not support metrics");
            } else {
                final MetricGroup kafkaMetricGroup = getRuntimeContext().getMetricGroup().addGroup("KafkaProducer");
                for (Map.Entry<MetricName, ? extends Metric> metric : metrics.entrySet()) {
                    kafkaMetricGroup.gauge(metric.getKey().name(), new KafkaMetricWrapper(metric.getValue()));
                }
            }
        }

        if (logFailuresOnly) {
            callback = new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null) {
                        LOG.error("Error while sending record to Kafka: " + e.getMessage(), e);
                    }
                    acknowledgeMessage();
                }
            };
        } else {
            callback = new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null && asyncException == null) {
                        asyncException = exception;
                    }
                    acknowledgeMessage();
                }
            };
        }
    }

    @Override
    public void close() throws IOException {
        schema.close();
        producer.close();
    }

    // ------------------- Logic for handling checkpoint flushing -------------------------- //

    private void acknowledgeMessage() {
        if (flushOnCheckpoint) {
            synchronized (pendingRecordsLock) {
                pendingRecords--;
                if (pendingRecords == 0) {
                    pendingRecordsLock.notifyAll();
                }
            }
        }
    }

    protected abstract void flush();

    @Override
    public void writeAddRecord(Row row) throws IOException {
        // propagate asynchronous errors
        checkErroneous();

        String targetTopic = schema.getTargetTopic(row);
        if (targetTopic == null) {
            targetTopic = defaultTopicId;
        }

        int[] partitions = this.topicPartitionsMap.get(targetTopic);
        if (null == partitions) {
            partitions = getPartitionsByTopic(targetTopic, producer);
            this.topicPartitionsMap.put(targetTopic, partitions);
        }

        ProducerRecord<byte[], byte[]> record;
        record = schema.convert(row, targetTopic, partitions);
        if (flushOnCheckpoint) {
            synchronized (pendingRecordsLock) {
                pendingRecords++;
            }
        }
        if (record != null) {
            producer.send(record, callback);
        }
    }

    @Override
    public void writeDeleteRecord(Row row) throws IOException {

    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void configure(Configuration parameters) {

    }
    // ----------------------------------- Utilities --------------------------

    protected void checkErroneous() {
        Exception e = asyncException;
        if (e != null) {
            // prevent double throwing
            asyncException = null;
            throw new RuntimeException("Failed to send data to Kafka: " + e.getMessage(), e);
        }
    }

    protected static int[] getPartitionsByTopic(String topic, KafkaProducer<byte[], byte[]> producer) {
        // the fetched list is immutable, so we're creating a mutable copy in order to sort it
        List<PartitionInfo> partitionsList = new ArrayList<>(producer.partitionsFor(topic));

        // sort the partitions by partition id to make sure the fetched partition list is the same across subtasks
        Collections.sort(partitionsList, new Comparator<PartitionInfo>() {
            @Override
            public int compare(PartitionInfo o1, PartitionInfo o2) {
                return Integer.compare(o1.partition(), o2.partition());
            }
        });

        int[] partitions = new int[partitionsList.size()];
        for (int i = 0; i < partitions.length; i++) {
            partitions[i] = partitionsList.get(i).partition();
        }

        return partitions;
    }
}
