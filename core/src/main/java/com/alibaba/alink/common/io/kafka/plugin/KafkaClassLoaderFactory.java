package com.alibaba.alink.common.io.kafka.plugin;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.exceptions.AkPluginErrorException;
import com.alibaba.alink.common.io.plugin.ClassLoaderContainer;
import com.alibaba.alink.common.io.plugin.ClassLoaderFactory;
import com.alibaba.alink.common.io.plugin.PluginDescriptor;
import com.alibaba.alink.common.io.plugin.PluginDistributeCache;
import com.alibaba.alink.common.io.plugin.RegisterKey;
import com.alibaba.alink.common.io.plugin.TemporaryClassLoaderContext;
import com.alibaba.alink.operator.stream.sink.KafkaSourceSinkFactory;

import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.function.Predicate;

public class KafkaClassLoaderFactory extends ClassLoaderFactory {
	public final static String KAFKA_NAME = "kafka";

	public KafkaClassLoaderFactory(String version) {
		super(new RegisterKey(KAFKA_NAME, version), PluginDistributeCache.createDistributeCache(KAFKA_NAME, version));
	}

	public static KafkaSourceSinkFactory create(KafkaClassLoaderFactory factory) {
		ClassLoader classLoader = factory.create();

		try (TemporaryClassLoaderContext context = TemporaryClassLoaderContext.of(classLoader)) {
			Iterator <KafkaSourceSinkFactory> iter = ServiceLoader
				.load(KafkaSourceSinkFactory.class, classLoader)
				.iterator();

			if (iter.hasNext()) {
				return iter.next();
			} else {
				throw new AkPluginErrorException("Could not find the class factory in classloader.");
			}
		}
	}

	@Override
	public ClassLoader create() {
		return ClassLoaderContainer.getInstance().create(
			registerKey,
			distributeCache,
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
