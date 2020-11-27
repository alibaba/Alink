package com.alibaba.alink.testutil.envfactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.reflections.Reflections;

public class EnvFactory implements BaseEnvFactory {

    private final static Map<String, Class<? extends BaseEnvFactory>> NAME_TO_ENV_FACTORY_IMPL = new HashMap<>();

    static {
        Reflections reflections = new Reflections("com.alibaba.alink");
        Set<Class<?>> classes = reflections.getTypesAnnotatedWith(EnvTypeAnnotation.class);
        for (Class<?> clz : classes) {
            if (BaseEnvFactory.class.isAssignableFrom(clz)) {
                String envTypeName = clz.getAnnotation(EnvTypeAnnotation.class).name();
                NAME_TO_ENV_FACTORY_IMPL.put(envTypeName, (Class<? extends BaseEnvFactory>) clz);
            }
        }
    }

    private BaseEnvFactory envFactoryImpl;

    @Override
    public void initialize(Properties properties) {
        String envType = properties.getProperty("envType", "local");
        Class<? extends BaseEnvFactory> envFactoryClass = NAME_TO_ENV_FACTORY_IMPL.get(envType);
        try {
            System.err.println(String.format("Initialize env factory for %s", envType));
            Constructor<? extends BaseEnvFactory> constructor = envFactoryClass.getConstructor();
            envFactoryImpl = constructor.newInstance();
        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new RuntimeException(String.format("Cannot initialize env factory for %s", envType), e);
        }
        envFactoryImpl.initialize(properties);
    }

    @Override
    public Object getMlEnv() {
        return envFactoryImpl.getMlEnv();
    }

    @Override
    public void destroy() {
        envFactoryImpl.destroy();
    }
}
