package com.aerospike.jdbc.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.query.IndexType;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.CatalogTableName;
import com.aerospike.jdbc.model.Pair;

import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.logging.Logger;

import static java.lang.String.format;

public class IndexCreateHandler extends BaseQueryHandler {

    private static final Logger logger = Logger.getLogger(IndexCreateHandler.class.getName());

    protected IndexCreateHandler(IAerospikeClient client, Statement statement) {
        super(client, statement);
    }

    @Override
    public Pair<ResultSet, Integer> execute(AerospikeQuery query) {
        logger.info("CREATE INDEX statement");
        if (query.getColumns().size() != 1) {
            throw new UnsupportedOperationException(
                    format("Multi-column index is not supported, got: %s", query.getColumns()));
        } else {
            client.createIndex(
                    null,
                    query.getCatalog(),
                    query.getTable(),
                    query.getIndex(),
                    query.getColumns().get(0),
                    getIndexType(query));
        }
        databaseMetadata.resetCatalogIndexes();
        return new Pair<>(emptyRecordSet(query), 1);
    }

    private IndexType getIndexType(AerospikeQuery query) {
        final CatalogTableName catalogTableName = new CatalogTableName(
                query.getCatalog(),
                query.getTable()
        );
        final String columnName = query.getColumns().get(0);
        return databaseMetadata.getSchemaBuilder().getSchema(catalogTableName).stream()
                .filter(dataColumn -> dataColumn.getName().equals(columnName))
                .findFirst()
                .map(dataColumn -> {
                    int columnType = dataColumn.getType();
                    switch (columnType) {
                        case Types.VARCHAR:
                            return IndexType.STRING;
                        case Types.BIGINT:
                        case Types.INTEGER:
                            return IndexType.NUMERIC;
                        default:
                            throw new UnsupportedOperationException(
                                    format("Secondary index is not supported for type: %d", columnType));
                    }
                })
                .orElseThrow(() -> new IllegalArgumentException(format("Column %s not found in %s",
                        columnName, query.getTable())));
    }
}
