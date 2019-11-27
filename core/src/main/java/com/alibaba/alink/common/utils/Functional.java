package com.alibaba.alink.common.utils;

import org.apache.flink.util.function.TriFunction;

import java.io.Serializable;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Functional interfaces.
 */
public class Functional {

    /**
     * The same as {@link java.util.function.Function} except that exception is allowed.
     *
     * @param <T> Type of input value.
     * @param <R> Return value type.
     * @param <E> Exception type.
     */
    @FunctionalInterface
    public interface ExceptionFunction<T, R, E extends Throwable> {

        /**
         * Applies this function to the given argument.
         *
         * @param value function argument.
         * @return the function result.
         * @throws E if exception occurs.
         */
        R apply(T value) throws E;
    }


    /**
     * The same as {@link java.util.function.BiConsumer} except that exception is allowed.
     *
     * @param <T> Type of first input value.
     * @param <U> Type of second input value.
     * @param <E> Type of exception.
     */
    @FunctionalInterface
    public interface ExceptionBiConsumer<T, U, E extends Throwable> {

        /**
         * Applies this function to the given arguments.
         *
         * @param t First input value.
         * @param u Second input value.
         * @throws E if exception occurs.
         */
        void accept(T t, U u) throws E;

    }

    /**
     * A serializable java.util.function.Function.
     *
     * @param <T> Type of input value.
     * @param <R> Return value type.
     */
    @FunctionalInterface
    public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {
    }

    /**
     * A serializable java.util.function.Function.
     *
     * @param <T> Type of first input value.
     * @param <U> Type of second input value.
     */
    @FunctionalInterface
    public interface SerializableBiFunction<T, U, R> extends BiFunction<T, U, R>, Serializable {
    }

    /**
     * A function that taks 4 arguments.
     *
     * @param <T> Type of first input value
     * @param <U> Type of second input value
     * @param <V> Type of third input value
     * @param <W> Type of forth input value
     * @param <W> Type of first returned value
     */
    @FunctionalInterface
    public interface SerializableTriFunction<T, U, V, W> extends TriFunction<T, U, V, W>, Serializable {
    }

    /**
     * A function that taks 4 arguments.
     *
     * @param <T> Type of first input value
     * @param <U> Type of second input value
     * @param <V> Type of third input value
     * @param <W> Type of forth input value
     * @param <R> Type of first returned value
     */
    @FunctionalInterface
    public interface QuadFunction<T, U, V, W, R> {

        /**
         * A function that taks 4 arguments.
         *
         * @param t the first function argument
         * @param u the second function argument
         * @param v the third function argument
         * @param w the fourth function argument
         * @return the function result
         */
        R apply(T t, U u, V v, W w);
    }
}
