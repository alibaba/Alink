package org.apache.flink.ml.api.misc.param;

/**
 * Interface for the object, which need set/get parameters.
 *
 * @param <T> the type of the object
 */
public interface WithParams<T> {

	Params getParams();

	default <V> T set(ParamInfo <V> info, V value) {
		try {
			getParams().set(info, value);
		} catch (Exception ex) {
			throw new IllegalArgumentException("In " + getClass().getSimpleName() + "," + ex.getMessage());
		}
		return (T) this;
	}

	default <V> V get(ParamInfo <V> info) {
		try {
			return getParams().get(info);
		} catch (Exception ex) {
			throw new IllegalArgumentException("In " + getClass().getSimpleName() + "," + ex.getMessage());
		}
	}
}
