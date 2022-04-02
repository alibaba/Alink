package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.api.misc.param.WithParams;
import org.apache.flink.table.catalog.ObjectPath;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.catalog.BaseCatalog;
import com.alibaba.alink.common.utils.JsonConverter;

import java.io.Serializable;

public interface HasCatalogObject<T> extends WithParams <T> {

	@NameCn("catalog object")
	@DescCn("catalog object")
	ParamInfo <String> CATALOG_OBJECT = ParamInfoFactory
		.createParamInfo("catalogObject", String.class)
		.setDescription("Object in catalog.")
		.setRequired()
		.build();

	default CatalogObject getCatalogObject() {
		return CatalogObject.deserialize(get(CATALOG_OBJECT));
	}

	default T setCatalogObject(CatalogObject catalogObject) {
		return set(CATALOG_OBJECT, catalogObject.serialize());
	}

	class CatalogObject implements Serializable {
		private static final long serialVersionUID = -2025614220715115450L;

		private final BaseCatalog catalog;
		private final ObjectPath objectPath;
		private final Params params;

		public CatalogObject(BaseCatalog catalog, ObjectPath objectPath) {
			this(catalog, objectPath, new Params());
		}

		public CatalogObject(BaseCatalog catalog, ObjectPath objectPath, Params params) {
			this.catalog = catalog;
			this.objectPath = objectPath;
			if (params == null) {
				this.params = new Params();
			} else {
				this.params = params;
			}
		}

		public BaseCatalog getCatalog() {
			return catalog;
		}

		public ObjectPath getObjectPath() {
			return objectPath;
		}

		public Params getParams() {
			return params;
		}

		public String serialize() {
			return JsonConverter.toJson(CatalogObjectJsonable.fromCatalogObject(this));
		}

		public static CatalogObject deserialize(String str) {
			return JsonConverter.fromJson(str, CatalogObjectJsonable.class).toCatalogObject();
		}

		private final static class CatalogObjectJsonable implements Serializable {
			private static final long serialVersionUID = -5779302321363161961L;

			private Params catalog;
			private String objectPath;
			private Params params;

			public CatalogObjectJsonable() {
			}

			public CatalogObjectJsonable(Params catalog, String objectPath, Params params) {
				this.catalog = catalog;
				this.objectPath = objectPath;
				this.params = params;
			}

			public static CatalogObjectJsonable fromCatalogObject(CatalogObject catalogObject) {
				return new CatalogObjectJsonable(
					catalogObject.getCatalog().getParams(),
					catalogObject.getObjectPath().getFullName(),
					catalogObject.getParams()
				);
			}

			public CatalogObject toCatalogObject() {
				try {
					return new CatalogObject(BaseCatalog.of(catalog), ObjectPath.fromString(objectPath), params);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}

	}
}
