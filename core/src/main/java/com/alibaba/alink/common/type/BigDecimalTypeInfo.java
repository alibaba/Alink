/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.alink.common.type;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BigDecComparator;
import org.apache.flink.api.common.typeutils.base.BigDecSerializer;

import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.util.Arrays;

/**
 * {@link TypeInformation} for {@link BigDecimal}.
 *
 * <p>It differs from {@link BasicTypeInfo#BIG_DEC_TYPE_INFO} in that: This type includes
 * `precision` and `scale`, similar to SQL DECIMAL.
 * <p>
 * NOTE: This class is copied and modified from a higher version of Flink source code. This class is modified to NOT
 * inherit {@link BasicTypeInfo}, otherwise
 * {@link org.apache.flink.table.calcite.FlinkTypeFactory#createTypeFromTypeInfo} will
 */
public class BigDecimalTypeInfo extends TypeInformation <BigDecimal> implements AtomicType <BigDecimal> {

    private static final long serialVersionUID = 1L;

    public static BigDecimalTypeInfo of(int precision, int scale) {
        return new BigDecimalTypeInfo(precision, scale);
    }

    public static BigDecimalTypeInfo of(BigDecimal value) {
        return of(value.precision(), value.scale());
    }

    private final int precision;

    private final int scale;

    public BigDecimalTypeInfo(int precision, int scale) {
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public String toString() {
        return String.format("Decimal(%d,%d)", precision(), scale());
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BigDecimalTypeInfo)) {
            return false;
        }
        BigDecimalTypeInfo that = (BigDecimalTypeInfo) obj;
        return this.precision() == that.precision() && this.scale() == that.scale();
    }

    @Override
    public int hashCode() {
        int h0 = this.getClass().getCanonicalName().hashCode();
        return Arrays.hashCode(new int[] {h0, precision(), scale()});
    }

    @Override
    public boolean canEqual(Object obj) {
        return false;
    }

    public int precision() {
        return precision;
    }

    public int scale() {
        return scale;
    }

    @Override
    public boolean isBasicType() {
        return true;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    public Class <BigDecimal> getTypeClass() {
        return BigDecimal.class;
    }

    @Override
    public boolean isKeyType() {
        return true;
    }

    @Override
    public TypeSerializer <BigDecimal> createSerializer(ExecutionConfig config) {
        return BigDecSerializer.INSTANCE;
    }

    private static <X> TypeComparator <X> instantiateComparator(Class <? extends TypeComparator <X>> comparatorClass,
                                                                boolean ascendingOrder) {
        try {
            Constructor <? extends TypeComparator <X>> constructor = comparatorClass.getConstructor(Boolean.TYPE);
            return constructor.newInstance(ascendingOrder);
        } catch (Exception e) {
            throw new RuntimeException("Could not initialize basic comparator " + comparatorClass.getName(), e);
        }
    }

    @Override
    public TypeComparator <BigDecimal> createComparator(boolean sortOrderAscending, ExecutionConfig executionConfig) {
        return instantiateComparator(BigDecComparator.class, sortOrderAscending);
    }
}
