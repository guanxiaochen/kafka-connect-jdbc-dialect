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
 * A {@link DatabaseDialect} for MySQL.
 */
public class PhoenixDatabaseDialect extends GenericDatabaseDialect {

    /**
     * Create a new dialect instance with the given connector configuration.
     *
     * @param config the connector configuration; may not be null
     */
    public PhoenixDatabaseDialect(AbstractConfig config) {
        super(config);
    }

    /**
     * The provider for {@link PhoenixDatabaseDialect}.
     */
    public static class Provider extends SubprotocolBasedProvider {
        public Provider() {
            super(PhoenixDatabaseDialect.class.getSimpleName(), "phoenix");
        }

        @Override
        public DatabaseDialect create(AbstractConfig config) {
            return new PhoenixDatabaseDialect(config);
        }
    }

    @Override
    public String buildInsertStatement(TableId table, Collection<ColumnId> keyColumns, Collection<ColumnId> nonKeyColumns) {
        return buildPhoenixUpsertStatement(table, keyColumns, nonKeyColumns);
    }

    @Override
    public String buildUpdateStatement(TableId table, Collection<ColumnId> keyColumns, Collection<ColumnId> nonKeyColumns) {
        return buildPhoenixUpsertStatement(table, keyColumns, nonKeyColumns);
    }

    @Override
    public String buildUpsertQueryStatement(TableId table, Collection<ColumnId> keyColumns, Collection<ColumnId> nonKeyColumns, TableDefinition definition) {
        return buildPhoenixUpsertStatement(table, keyColumns, nonKeyColumns);
    }

    private String buildPhoenixUpsertStatement(TableId table, Collection<ColumnId> keyColumns, Collection<ColumnId> nonKeyColumns) {
        ExpressionBuilder builder = expressionBuilder();
        builder.append("UPSERT INTO ");
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
        return (builder, input) -> builder.appendColumnName(input.name().toUpperCase());
    }

    public String buildCreateTableStatement(TableId table, Collection<SinkRecordField> fields) {
        ExpressionBuilder builder = this.expressionBuilder();
        List<String> pkFieldNames = this.extractPrimaryKeyFieldNames(fields);
        builder.append("CREATE TABLE ");
        builder.append(table);
        builder.append(" (");
        this.writeColumnsSpec(builder, fields);
        if (!pkFieldNames.isEmpty()) {
            builder.append(",");
            builder.append(System.lineSeparator());
            builder.append("CONSTRAINT pk PRIMARY KEY(");
            builder.appendList().delimitedBy(",").transformedBy(ExpressionBuilder.quote()).of(pkFieldNames);
            builder.append(")");
        }

        builder.append(")");
        return builder.toString();
    }

    protected void writeColumnSpec(ExpressionBuilder builder,  SinkRecordField f) {
        builder.appendColumnName(f.name());
        builder.append(" ");
        builder.append(getSqlType(f));
        if (f.isPrimaryKey() && !f.schema().isOptional()) {
            builder.append(" NOT NULL");
        }
        if (f.defaultValue() != null) {
            builder.append(" DEFAULT ");
            formatColumnValue(
                    builder,
                    f.schemaName(),
                    f.schemaParameters(),
                    f.schemaType(),
                    f.defaultValue()
            );
        }
    }

    @Override
    protected String getSqlType(SinkRecordField field) {
        if (field.schemaName() != null) {
            switch (field.schemaName()) {
                case Decimal.LOGICAL_NAME:
                    // Maximum precision supported by Phoenix is 38
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
                return "INTEGER";
            case INT64:
                return "BIGINT";
            case FLOAT32:
                return "FLOAT";
            case FLOAT64:
                return "DOUBLE";
            case BOOLEAN:
                return "BOOLEAN";
            case STRING:
                return "VARCHAR";
            case BYTES:
                return "VARBINARY";
            default:
                return super.getSqlType(field);
        }
    }
}
