/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.util.Collection;
import java.util.List;

/**
 * A {@link DatabaseDialect} for Hive.
 */
public class HiveDatabaseDialect extends GenericDatabaseDialect {

    /**
     * Create a new dialect instance with the given connector configuration.
     *
     * @param config the connector configuration; may not be null
     */
    public HiveDatabaseDialect(AbstractConfig config) {
        super(config);
    }

    /**
     * The provider for {@link HiveDatabaseDialect}.
     */
    public static class Provider extends SubprotocolBasedProvider {
        public Provider() {
            super(HiveDatabaseDialect.class.getSimpleName(), "hive2");
        }

        @Override
        public DatabaseDialect create(AbstractConfig config) {
            return new HiveDatabaseDialect(config);
        }
    }

    @Override
    public String buildInsertStatement(TableId table, Collection<ColumnId> keyColumns, Collection<ColumnId> nonKeyColumns) {
        return buildHiveUpsertStatement(table, keyColumns, nonKeyColumns);
    }

    @Override
    public String buildUpdateStatement(TableId table, Collection<ColumnId> keyColumns, Collection<ColumnId> nonKeyColumns) {
        return buildHiveUpsertStatement(table, keyColumns, nonKeyColumns);
    }

    @Override
    public String buildUpsertQueryStatement(TableId table, Collection<ColumnId> keyColumns, Collection<ColumnId> nonKeyColumns, TableDefinition definition) {
        return buildHiveUpsertStatement(table, keyColumns, nonKeyColumns);
    }

    private String buildHiveUpsertStatement(TableId table, Collection<ColumnId> keyColumns, Collection<ColumnId> nonKeyColumns) {
        ExpressionBuilder builder = expressionBuilder();
        builder.append("INSERT INTO ");
        builder.append(table);
        builder.append("(");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy(columnNames())
                .of(keyColumns, nonKeyColumns);
        builder.append(") VALUES(");
        builder.appendMultiple(",", "?", keyColumns.size() + nonKeyColumns.size());
        builder.append(")");
        return builder.toString();
    }

    public static ExpressionBuilder.Transform<ColumnId> columnNames() {
        return (builder, input) -> builder.appendColumnName(input.name().toLowerCase());
    }

    public String buildCreateTableStatement(TableId table, Collection<SinkRecordField> fields) {
        ExpressionBuilder builder = this.expressionBuilder();
        builder.append("CREATE TABLE ");
        builder.append(table);
        builder.append(" (");
        this.writeColumnsSpec(builder, fields);
        builder.append(") stored AS PARQUET");
        return builder.toString();
    }

    protected void writeColumnSpec(ExpressionBuilder builder,  SinkRecordField f) {
        builder.appendColumnName(f.name());
        builder.append(" ");
        builder.append(getSqlType(f));
    }

    @Override
    protected String getSqlType(SinkRecordField field) {
        if (field.schemaName() != null) {
            switch (field.schemaName()) {
                case Decimal.LOGICAL_NAME:
                    int scale = Integer.parseInt(field.schemaParameters().get(Decimal.SCALE_FIELD));
                    return "DECIMAL(38," + scale + ")";
                case Date.LOGICAL_NAME:
                    return "DATE";
                case Time.LOGICAL_NAME:
                    return "TIME";
                case Timestamp.LOGICAL_NAME:
                    return "TIMESTAMP";
                default:
                    // pass through to primitive types
            }
        }
        switch (field.schemaType()) {
            case INT8:
                return "TINYINT";
            case INT16:
                return "SMALLINT";
            case INT32:
                return "INT";
            case INT64:
                return "BIGINT";
            case FLOAT32:
                return "FLOAT";
            case FLOAT64:
                return "DOUBLE";
            case BOOLEAN:
                return "BOOLEAN";
            case STRING:
                return "STRING";
            case BYTES:
                return "BINARY";
            default:
                return super.getSqlType(field);
        }
    }
}
