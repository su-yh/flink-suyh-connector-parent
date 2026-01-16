// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.flink.tools.cdc.mysql;

import org.apache.doris.flink.catalog.doris.DorisType;
import org.apache.doris.flink.tools.cdc.DuckdbType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.util.Preconditions;

import java.math.BigDecimal;

import static org.apache.doris.flink.catalog.DorisTypeMapper.MAX_SUPPORTED_DATE_TIME_PRECISION;

public class MysqlType {

    // MySQL driver returns width of timestamp types instead of precision.
    // 19 characters are used for zero-precision timestamps while others
    // require 19 + precision + 1 characters with the additional character
    // required for the decimal separator.
    private static final int ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE = 19;
    private static final String BIT = "BIT";
    private static final String BOOLEAN = "BOOLEAN";
    private static final String BOOL = "BOOL";
    private static final String TINYINT = "TINYINT";
    private static final String TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    private static final String TINYINT_UNSIGNED_ZEROFILL = "TINYINT UNSIGNED ZEROFILL";
    private static final String SMALLINT = "SMALLINT";
    private static final String SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    private static final String SMALLINT_UNSIGNED_ZEROFILL = "SMALLINT UNSIGNED ZEROFILL";
    private static final String MEDIUMINT = "MEDIUMINT";
    private static final String MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    private static final String MEDIUMINT_UNSIGNED_ZEROFILL = "MEDIUMINT UNSIGNED ZEROFILL";
    private static final String INT = "INT";
    private static final String INT_UNSIGNED = "INT UNSIGNED";
    private static final String INT_UNSIGNED_ZEROFILL = "INT UNSIGNED ZEROFILL";
    private static final String INTEGER = "INTEGER";
    private static final String INTEGER_UNSIGNED = "INTEGER UNSIGNED";
    private static final String INTEGER_UNSIGNED_ZEROFILL = "INTEGER UNSIGNED ZEROFILL";
    private static final String BIGINT = "BIGINT";
    private static final String SERIAL = "SERIAL";
    private static final String BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    private static final String BIGINT_UNSIGNED_ZEROFILL = "BIGINT UNSIGNED ZEROFILL";
    private static final String REAL = "REAL";
    private static final String REAL_UNSIGNED = "REAL UNSIGNED";
    private static final String REAL_UNSIGNED_ZEROFILL = "REAL UNSIGNED ZEROFILL";
    private static final String FLOAT = "FLOAT";
    private static final String FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    private static final String FLOAT_UNSIGNED_ZEROFILL = "FLOAT UNSIGNED ZEROFILL";
    private static final String DOUBLE = "DOUBLE";
    private static final String DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";
    private static final String DOUBLE_UNSIGNED_ZEROFILL = "DOUBLE UNSIGNED ZEROFILL";
    private static final String DOUBLE_PRECISION = "DOUBLE PRECISION";
    private static final String DOUBLE_PRECISION_UNSIGNED = "DOUBLE PRECISION UNSIGNED";
    private static final String DOUBLE_PRECISION_UNSIGNED_ZEROFILL =
            "DOUBLE PRECISION UNSIGNED ZEROFILL";
    private static final String NUMERIC = "NUMERIC";
    private static final String NUMERIC_UNSIGNED = "NUMERIC UNSIGNED";
    private static final String NUMERIC_UNSIGNED_ZEROFILL = "NUMERIC UNSIGNED ZEROFILL";
    private static final String FIXED = "FIXED";
    private static final String FIXED_UNSIGNED = "FIXED UNSIGNED";
    private static final String FIXED_UNSIGNED_ZEROFILL = "FIXED UNSIGNED ZEROFILL";
    private static final String DECIMAL = "DECIMAL";
    private static final String DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    private static final String DECIMAL_UNSIGNED_ZEROFILL = "DECIMAL UNSIGNED ZEROFILL";
    private static final String CHAR = "CHAR";
    private static final String VARCHAR = "VARCHAR";
    private static final String TINYTEXT = "TINYTEXT";
    private static final String MEDIUMTEXT = "MEDIUMTEXT";
    private static final String TEXT = "TEXT";
    private static final String LONGTEXT = "LONGTEXT";
    private static final String DATE = "DATE";
    private static final String TIME = "TIME";
    private static final String DATETIME = "DATETIME";
    private static final String TIMESTAMP = "TIMESTAMP";
    private static final String YEAR = "YEAR";
    private static final String BINARY = "BINARY";
    private static final String VARBINARY = "VARBINARY";
    private static final String TINYBLOB = "TINYBLOB";
    private static final String MEDIUMBLOB = "MEDIUMBLOB";
    private static final String BLOB = "BLOB";
    private static final String LONGBLOB = "LONGBLOB";
    private static final String JSON = "JSON";
    private static final String ENUM = "ENUM";
    private static final String SET = "SET";

    public static String toDorisType(String type, Integer length, Integer scale) {
        switch (type.toUpperCase()) {
            case BIT:
            case BOOLEAN:
            case BOOL:
                return DorisType.BOOLEAN;
            case TINYINT:
                return DorisType.TINYINT;
            case TINYINT_UNSIGNED:
            case TINYINT_UNSIGNED_ZEROFILL:
            case SMALLINT:
                return DorisType.SMALLINT;
            case SMALLINT_UNSIGNED:
            case SMALLINT_UNSIGNED_ZEROFILL:
            case INT:
            case INTEGER:
            case MEDIUMINT:
            case YEAR:
                return DorisType.INT;
            case INT_UNSIGNED:
            case INT_UNSIGNED_ZEROFILL:
            case INTEGER_UNSIGNED:
            case INTEGER_UNSIGNED_ZEROFILL:
            case MEDIUMINT_UNSIGNED:
            case MEDIUMINT_UNSIGNED_ZEROFILL:
            case BIGINT:
                return DorisType.BIGINT;
            case BIGINT_UNSIGNED:
            case BIGINT_UNSIGNED_ZEROFILL:
                return DorisType.LARGEINT;
            case FLOAT:
            case FLOAT_UNSIGNED:
            case FLOAT_UNSIGNED_ZEROFILL:
                return DorisType.FLOAT;
            case REAL:
            case REAL_UNSIGNED:
            case REAL_UNSIGNED_ZEROFILL:
            case DOUBLE:
            case DOUBLE_UNSIGNED:
            case DOUBLE_UNSIGNED_ZEROFILL:
            case DOUBLE_PRECISION:
            case DOUBLE_PRECISION_UNSIGNED:
            case DOUBLE_PRECISION_UNSIGNED_ZEROFILL:
                return DorisType.DOUBLE;
            case NUMERIC:
            case NUMERIC_UNSIGNED:
            case NUMERIC_UNSIGNED_ZEROFILL:
            case FIXED:
            case FIXED_UNSIGNED:
            case FIXED_UNSIGNED_ZEROFILL:
            case DECIMAL:
            case DECIMAL_UNSIGNED:
            case DECIMAL_UNSIGNED_ZEROFILL:
                return length != null && length <= 38
                        ? String.format(
                                "%s(%s,%s)",
                                DorisType.DECIMAL_V3,
                                length,
                                scale != null && scale >= 0 ? scale : 0)
                        : DorisType.STRING;
            case DATE:
                return DorisType.DATE_V2;
            case DATETIME:
            case TIMESTAMP:
                // default precision is 0
                // see https://dev.mysql.com/doc/refman/8.0/en/date-and-time-type-syntax.html
                if (length == null
                        || length <= 0
                        || length == ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE) {
                    return String.format("%s(%s)", DorisType.DATETIME_V2, 0);
                } else if (length > ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE + 1) {
                    // Timestamp with a fraction of seconds.
                    // For example, 2024-01-01 01:01:01.1
                    // The decimal point will occupy 1 character.
                    // Thus,the length of the timestamp is 21.
                    return String.format(
                            "%s(%s)",
                            DorisType.DATETIME_V2,
                            Math.min(
                                    length - ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE - 1,
                                    MAX_SUPPORTED_DATE_TIME_PRECISION));
                } else if (length <= TimestampType.MAX_PRECISION) {
                    // For Debezium JSON data, the timestamp/datetime length ranges from 0 to 9.
                    return String.format(
                            "%s(%s)",
                            DorisType.DATETIME_V2,
                            Math.min(length, MAX_SUPPORTED_DATE_TIME_PRECISION));
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported length: "
                                    + length
                                    + " for MySQL TIMESTAMP/DATETIME types");
                }
            case CHAR:
            case VARCHAR:
                Preconditions.checkNotNull(length);
                return length * 3 > 65533
                        ? DorisType.STRING
                        : String.format("%s(%s)", DorisType.VARCHAR, length * 3);
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case ENUM:
            case TIME:
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
            case BINARY:
            case VARBINARY:
            case SET:
                return DorisType.STRING;
            case JSON:
                return DorisType.JSONB;
            default:
                throw new UnsupportedOperationException("Unsupported MySQL Type: " + type);
        }
    }

    // ---------------------- 新增 toDuckdbType 方法 ----------------------
    /**
     * 将 MySQL 字段类型映射为 DuckDB 字段类型
     * @param type MySQL 字段类型名称（如 INT、VARCHAR、DECIMAL）
     * @param length 字段长度（如 VARCHAR(50) 的 50，DECIMAL(10,2) 的 10）
     * @param scale 字段小数位数（如 DECIMAL(10,2) 的 2）
     * @return DuckDB 对应的字段类型字符串
     */
    public static String toDuckdbType(String type, Integer length, Integer scale) {
        // 统一转为大写，避免大小写敏感问题
        String mysqlType = type.toUpperCase();

        switch (mysqlType) {
            // 布尔类型映射
            case BIT:
            case BOOLEAN:
            case BOOL:
                return DuckdbType.BOOLEAN;

            // 整数类型映射（兼容无符号/零填充，DuckDB 无专门无符号类型，使用更大范围整数兼容）
            case TINYINT:
                return DuckdbType.TINYINT;
            case TINYINT_UNSIGNED:
            case TINYINT_UNSIGNED_ZEROFILL:
            case SMALLINT:
                return DuckdbType.SMALLINT;
            case SMALLINT_UNSIGNED:
            case SMALLINT_UNSIGNED_ZEROFILL:
            case MEDIUMINT:
            case YEAR:
                return DuckdbType.INTEGER;
            case INT:
            case INTEGER:
            case INT_UNSIGNED:
            case INT_UNSIGNED_ZEROFILL:
            case INTEGER_UNSIGNED:
            case INTEGER_UNSIGNED_ZEROFILL:
            case MEDIUMINT_UNSIGNED:
            case MEDIUMINT_UNSIGNED_ZEROFILL:
                return DuckdbType.INTEGER;
            case BIGINT:
            case SERIAL:
                return DuckdbType.BIGINT;
            case BIGINT_UNSIGNED:
            case BIGINT_UNSIGNED_ZEROFILL:
                return DuckdbType.BIGINT;

            // 浮点类型映射
            case FLOAT:
            case FLOAT_UNSIGNED:
            case FLOAT_UNSIGNED_ZEROFILL:
            case REAL:
            case REAL_UNSIGNED:
            case REAL_UNSIGNED_ZEROFILL:
                return DuckdbType.FLOAT;
            case DOUBLE:
            case DOUBLE_UNSIGNED:
            case DOUBLE_UNSIGNED_ZEROFILL:
            case DOUBLE_PRECISION:
            case DOUBLE_PRECISION_UNSIGNED:
            case DOUBLE_PRECISION_UNSIGNED_ZEROFILL:
                return DuckdbType.DOUBLE;

            // 高精度小数类型映射（DECIMAL/NUMERIC/FIXED 统一映射为 DuckDB DECIMAL）
            case NUMERIC:
            case NUMERIC_UNSIGNED:
            case NUMERIC_UNSIGNED_ZEROFILL:
            case FIXED:
            case FIXED_UNSIGNED:
            case FIXED_UNSIGNED_ZEROFILL:
            case DECIMAL:
            case DECIMAL_UNSIGNED:
            case DECIMAL_UNSIGNED_ZEROFILL:
                // 处理默认值：长度默认 10，小数位默认 0
                int decimalLength = (length != null && length > 0) ? length : 10;
                int decimalScale = (scale != null && scale >= 0) ? scale : 0;
                // DuckDB DECIMAL 支持 (precision, scale)，precision 最大 38
                decimalLength = Math.min(decimalLength, 38);
                return String.format("DECIMAL(%d, %d)", decimalLength, decimalScale);

            // 日期时间类型映射
            case DATE:
                return DuckdbType.DATE;
            case TIME:
                return DuckdbType.TIME;
            case DATETIME:
            case TIMESTAMP:
                // 处理时间精度，默认 0 位小数（无毫秒）
                if (length == null || length <= 0 || length == ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE) {
                    return DuckdbType.TIMESTAMP;
                } else {
                    // 提取毫秒精度，最大支持 6 位（DuckDB 标准）
                    int precision = Math.min(length - ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE - 1, 6);
                    return String.format("TIMESTAMP(%d)", precision);
                }

                // 字符串类型映射
            case CHAR:
                length = (length != null && length > 0) ? length : 1;
                return String.format("CHAR(%d)", length);
            case VARCHAR:
                Preconditions.checkNotNull(length, "VARCHAR type must specify length");
                return String.format("VARCHAR(%d)", length);
            case TINYTEXT:
            case MEDIUMTEXT:
            case TEXT:
            case LONGTEXT:
                return DuckdbType.VARCHAR; // DuckDB 无专门 TEXT 类型，使用无长度限制 VARCHAR 兼容

            // 二进制类型映射（DuckDB 用 BLOB 统一兼容各类二进制数据）
            case BINARY:
            case VARBINARY:
            case TINYBLOB:
            case MEDIUMBLOB:
            case BLOB:
            case LONGBLOB:
                return DuckdbType.BLOB;

            // 特殊类型映射
            case JSON:
                return DuckdbType.JSON;
            case ENUM:
            case SET:
                return DuckdbType.VARCHAR; // ENUM/SET 转为字符串类型兼容

            // 未匹配类型抛出异常
            default:
                throw new UnsupportedOperationException("Unsupported MySQL Type for DuckDB mapping: " + type);
        }
    }

    /**
     * 将 MySQL 字段类型映射为 DuckDB 字段类型
     * @param type MySQL 字段类型名称（如 INT、VARCHAR、DECIMAL）
     * @param length 字段长度（如 VARCHAR(50) 的 50，DECIMAL(10,2) 的 10）
     * @param scale 字段小数位数（如 DECIMAL(10,2) 的 2）
     * @return DuckDB 对应的字段类型字符串
     */
    public static Class<?> toJavaClass(String type, Integer length, Integer scale) {
        // 统一转为大写，避免大小写敏感问题
        String mysqlType = type.toUpperCase();

        switch (mysqlType) {
            // 布尔类型映射
            case BIT:
            case BOOLEAN:
            case BOOL:
            // 整数类型映射（兼容无符号/零填充，DuckDB 无专门无符号类型，使用更大范围整数兼容）
            case TINYINT:
            case TINYINT_UNSIGNED:
            case TINYINT_UNSIGNED_ZEROFILL:
            case SMALLINT:
            case SMALLINT_UNSIGNED:
            case SMALLINT_UNSIGNED_ZEROFILL:
            case MEDIUMINT:
            case YEAR:
                return Integer.class;
            case INT:
            case INTEGER:
            case INT_UNSIGNED:
            case INT_UNSIGNED_ZEROFILL:
            case INTEGER_UNSIGNED:
            case INTEGER_UNSIGNED_ZEROFILL:
            case MEDIUMINT_UNSIGNED:
            case MEDIUMINT_UNSIGNED_ZEROFILL:
            case BIGINT:
            case SERIAL:
            case BIGINT_UNSIGNED:
            case BIGINT_UNSIGNED_ZEROFILL:
                return Integer.class;

            // 浮点类型映射
            case FLOAT:
            case FLOAT_UNSIGNED:
            case FLOAT_UNSIGNED_ZEROFILL:
            case REAL:
            case REAL_UNSIGNED:
            case REAL_UNSIGNED_ZEROFILL:
                return Float.class;
            case DOUBLE:
            case DOUBLE_UNSIGNED:
            case DOUBLE_UNSIGNED_ZEROFILL:
            case DOUBLE_PRECISION:
            case DOUBLE_PRECISION_UNSIGNED:
            case DOUBLE_PRECISION_UNSIGNED_ZEROFILL:
                return Double.class;

            // 高精度小数类型映射（DECIMAL/NUMERIC/FIXED 统一映射为 DuckDB DECIMAL）
            case NUMERIC:
            case NUMERIC_UNSIGNED:
            case NUMERIC_UNSIGNED_ZEROFILL:
            case FIXED:
            case FIXED_UNSIGNED:
            case FIXED_UNSIGNED_ZEROFILL:
            case DECIMAL:
            case DECIMAL_UNSIGNED:
            case DECIMAL_UNSIGNED_ZEROFILL:
                return BigDecimal.class;

            // 日期时间类型映射
            case DATE:
            case TIME:
            case DATETIME:
            case TIMESTAMP:
                // return Date.class; // 时间相关的都使用字符串来处理，免去时区的问题
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case MEDIUMTEXT:
            case TEXT:
            case LONGTEXT:
                return String.class;

            // 二进制类型映射（DuckDB 用 BLOB 统一兼容各类二进制数据）
            case BINARY:
            case VARBINARY:
            case TINYBLOB:
            case MEDIUMBLOB:
            case BLOB:
            case LONGBLOB:
                return Boolean.class;

            // 特殊类型映射
            case JSON:
            case ENUM:
            case SET:
                return String.class;

            // 未匹配类型抛出异常
            default:
                throw new UnsupportedOperationException("Unsupported MySQL Type for DuckDB mapping: " + type);
        }
    }
}