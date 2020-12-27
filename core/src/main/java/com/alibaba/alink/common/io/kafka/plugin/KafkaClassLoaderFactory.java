package com.alibaba.alink.common.io.kafka.plugin;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.io.plugin.ClassLoaderContainer;
import com.alibaba.alink.common.io.plugin.ClassLoaderFactory;
import com.alibaba.alink.common.io.plugin.PluginDescriptor;
import com.alibaba.alink.common.io.plugin.RegisterKey;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Function;
import java.util.function.Predicate;

public class KafkaClassLoaderFactory extends ClassLoaderFactory {
	public final static String KAFKA_NAME = "kafka";

	public KafkaClassLoaderFactory(String version) {
		super(new RegisterKey(KAFKA_NAME, version), ClassLoaderContainer.createPluginContextOnClient());
	}

	public static KafkaSourceSinkFactory create(KafkaClassLoaderFactory factory) {
		try {
			return (KafkaSourceSinkFactory) factory
				.create()
				.loadClass("com.alibaba.alink.common.io.kafka.plugin.KafkaSourceSinkInPluginFactory")
				.getConstructor()
				.newInstance();
		} catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException |
			ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public ClassLoader create() {
		return ClassLoaderContainer.getInstance().create(
			registerKey,
			registerContext,
			KafkaSourceSinkFactory.class,
			new KafkaServiceFilter(),
			new KafkaVersionGetter()
		);
	}

	private static class KafkaServiceFilter implements Predicate <KafkaSourceSinkFactory> {

		@Override
		public boolean test(KafkaSourceSinkFactory factory) {
			return true;
		}
	}

	private static class KafkaVersionGetter implements
		Function <Tuple2 <KafkaSourceSinkFactory, PluginDescriptor>, String> {

		@Override
		public String apply(Tuple2 <KafkaSourceSinkFactory, PluginDescriptor> factory) {
			return factory.f1.getVersion();
		}
	}

}
