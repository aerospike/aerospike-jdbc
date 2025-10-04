package com.aerospike.jdbc.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.AerospikeSecondaryIndex;
import com.aerospike.jdbc.model.DataColumn;
import com.aerospike.jdbc.model.DriverPolicy;
import com.aerospike.jdbc.model.Pair;
import com.aerospike.jdbc.sql.ListRecordSet;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Logger;

import static com.aerospike.jdbc.util.AerospikeUtils.hasSetIndex;
import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

public class ExplainQueryHandler extends SelectQueryHandler {

    private static final Logger logger = Logger.getLogger(ExplainQueryHandler.class.getName());

    private static final String EXPLAIN_CATALOG = "system";
    private static final String EXPLAIN_TABLE = "explain";
    private static final List<DataColumn> explainColumns;

    static {
        String[] columnNames = new String[]{"COMMAND_TYPE", "SET_NAME", "INDEX_TYPE", "INDEX_NAME",
                "BIN_NAME", "ENTRIES_PER_VALUE"};
        int[] columnTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, INTEGER};

        explainColumns = range(0, columnNames.length).boxed()
                .map(i -> new DataColumn(EXPLAIN_CATALOG, EXPLAIN_TABLE,
                        columnNames[i], columnNames[i]).withType(columnTypes[i]))
                .collect(toList());
    }

    protected ExplainQueryHandler(IAerospikeClient client, Statement statement, DriverPolicy driverPolicy) {
        super(client, statement, driverPolicy);
    }

    @Override
    public Pair<ResultSet, Integer> execute(AerospikeQuery query) {
        logger.info("EXPLAIN statement");

        final List<List<?>> explainResult = new ArrayList<>();
        Optional<AerospikeSecondaryIndex> indexOptional = secondaryIndex(query);
        if (indexOptional.isPresent()) {
            AerospikeSecondaryIndex idx = indexOptional.get();
            explainResult.add(asList("si_query", idx.getSet(), idx.getIndexType(), idx.getIndexName(),
                    idx.getBinName(), idx.getBinValuesRatio()));
        } else if (hasSetIndex(client, query.getCatalog(), query.getTable())) {
            explainResult.add(asList("set_query", query.getTable(), "set_index", null, null, 1));
        } else if (!query.getPrimaryKeys().isEmpty()) {
            explainResult.add(asList("key_query", query.getTable(), "primary_index", null, null, 1));
        } else if (query.isCount() && Objects.isNull(query.getPredicate())) {
            explainResult.add(asList("info_query", query.getTable(), null, null, null, 0));
        } else {
            explainResult.add(asList("pi_query", query.getTable(), "primary_index", null, null, 1));
        }

        ResultSet resultSet = new ListRecordSet(null, EXPLAIN_CATALOG, EXPLAIN_TABLE,
                explainColumns, explainResult);

        return new Pair<>(resultSet, -1);
    }
}
