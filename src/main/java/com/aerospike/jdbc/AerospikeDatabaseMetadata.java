package com.aerospike.jdbc;

import com.aerospike.jdbc.model.AerospikeClusterInfo;
import com.aerospike.jdbc.model.AerospikeSecondaryIndex;
import com.aerospike.jdbc.model.DataColumn;
import com.aerospike.jdbc.schema.AerospikeSchemaBuilder;
import com.aerospike.jdbc.sql.ListRecordSet;
import com.aerospike.jdbc.sql.SimpleWrapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.aerospike.jdbc.util.AerospikeUtils.getCatalogIndexes;
import static com.aerospike.jdbc.util.AerospikeUtils.getClusterInfo;
import static com.aerospike.jdbc.util.Constants.DRIVER_MAJOR_VERSION;
import static com.aerospike.jdbc.util.Constants.DRIVER_MINOR_VERSION;
import static com.aerospike.jdbc.util.Constants.DRIVER_VERSION;
import static com.aerospike.jdbc.util.Constants.JDBC_MAJOR_VERSION;
import static com.aerospike.jdbc.util.Constants.JDBC_MINOR_VERSION;
import static com.aerospike.jdbc.util.Constants.PRIMARY_KEY_COLUMN_NAME;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.sql.Connection.TRANSACTION_NONE;
import static java.sql.Connection.TRANSACTION_SERIALIZABLE;
import static java.sql.Types.*;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

@SuppressWarnings("java:S1192")
public class AerospikeDatabaseMetadata implements DatabaseMetaData, SimpleWrapper {

    private static final Logger logger = Logger.getLogger(AerospikeDatabaseMetadata.class.getName());

    private final String url;
    private final AerospikeConnection connection;

    private final AerospikeClusterInfo clusterInfo;
    private final AerospikeSchemaBuilder schemaBuilder;
    private final Cache<String, ResultSetMetaData> resultSetMetaDataCache;

    private volatile Map<String, Collection<AerospikeSecondaryIndex>> catalogIndexes;

    public AerospikeDatabaseMetadata(String url, AerospikeConnection connection) {
        logger.info("Init AerospikeDatabaseMetadata");
        this.url = url;
        this.connection = connection;

        clusterInfo = getClusterInfo(connection.getClient());
        schemaBuilder = new AerospikeSchemaBuilder(
                connection.getClient(),
                connection.getConfiguration().getDriverPolicy()
        );
        resultSetMetaDataCache = CacheBuilder.newBuilder().build();
    }

    public AerospikeSchemaBuilder getSchemaBuilder() {
        return schemaBuilder;
    }

    public void resetCatalogIndexes() {
        logger.fine(() -> "Reset secondary index information");
        catalogIndexes = null;
    }

    public Collection<AerospikeSecondaryIndex> getSecondaryIndexes(String catalog) {
        initCatalogIndexes();
        return catalogIndexes.get(catalog);
    }

    private void initCatalogIndexes() {
        if (catalogIndexes == null) {
            synchronized (this) {
                if (catalogIndexes == null) {
                    logger.info(() -> "Load secondary index information");
                    catalogIndexes = getCatalogIndexes(connection.getClient(), connection.getAerospikeVersion());
                }
            }
        }
    }

    @Override
    public boolean allProceduresAreCallable() {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() {
        return true;
    }

    @Override
    public String getURL() {
        return url;
    }

    @Override
    public String getUserName() throws SQLException {
        return connection.getClientInfo().getProperty("user");
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtStart() {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtEnd() {
        return false;
    }

    @Override
    public String getDatabaseProductName() {
        return clusterInfo.getEdition();
    }

    @Override
    public String getDatabaseProductVersion() {
        return clusterInfo.getBuild();
    }

    @Override
    public String getDriverName() {
        return AerospikeDriver.class.getName();
    }

    @Override
    public String getDriverVersion() {
        return DRIVER_VERSION;
    }

    @Override
    public int getDriverMajorVersion() {
        return DRIVER_MAJOR_VERSION;
    }

    @Override
    public int getDriverMinorVersion() {
        return DRIVER_MINOR_VERSION;
    }

    @Override
    public boolean usesLocalFiles() {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public String getIdentifierQuoteString() {
        return "\"";
    }

    @Override
    public String getSQLKeywords() {
        return "truncate";
    }

    @Override
    public String getNumericFunctions() {
        return "";
    }

    @Override
    public String getStringFunctions() {
        return "";
    }

    @Override
    public String getSystemFunctions() {
        return "";
    }

    @Override
    public String getTimeDateFunctions() {
        return "";
    }

    @Override
    public String getSearchStringEscape() {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() {
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() {
        return false;
    }

    @Override
    public boolean supportsConvert() {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() {
        return false;
    }

    @Override
    public boolean supportsGroupBy() {
        return false;
    }

    @Override
    public boolean supportsGroupByUnrelated() {
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() {
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() {
        return true;
    }

    @Override
    public boolean supportsNonNullableColumns() {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() {
        return false;
    }

    @Override
    public String getSchemaTerm() {
        return "";
    }

    @Override
    public String getProcedureTerm() {
        return "";
    }

    @Override
    public String getCatalogTerm() {
        return "namespace";
    }

    @Override
    public boolean isCatalogAtStart() {
        return true;
    }

    @Override
    public String getCatalogSeparator() {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() {
        return true;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return false;
    }

    @Override
    public boolean supportsUnion() {
        return true;
    }

    @Override
    public boolean supportsUnionAll() {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() {
        return 14;
    }

    @Override
    public int getMaxCharLiteralLength() {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() {
        return 14;
    }

    @Override
    public int getMaxColumnsInGroupBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() {
        return 1;
    }

    @Override
    public int getMaxColumnsInOrderBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() {
        return 32767;
    }

    @Override
    public int getMaxColumnsInTable() {
        return 32767;
    }

    @Override
    public int getMaxConnections() {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() {
        return 0;
    }

    @Override
    public int getMaxIndexLength() {
        return 256;
    }

    @Override
    public int getMaxSchemaNameLength() {
        return 14;
    }

    @Override
    public int getMaxProcedureNameLength() {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() {
        return 14;
    }

    @Override
    public int getMaxRowSize() {
        return 8 * 1024 * 1024;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() {
        return false;
    }

    @Override
    public int getMaxStatementLength() {
        return 0;
    }

    @Override
    public int getMaxStatements() {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() {
        return 63;
    }

    @Override
    public int getMaxTablesInSelect() {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() {
        return 63;
    }

    @Override
    public int getDefaultTransactionIsolation() {
        return TRANSACTION_SERIALIZABLE;
    }

    @Override
    public boolean supportsTransactions() {
        return true;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) {
        return TRANSACTION_NONE == level || TRANSACTION_SERIALIZABLE == level;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() {
        return true;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) {
        String[] columns = new String[]{"PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "reserved1", "reserved2",
                "reserved3", "REMARKS", "PROCEDURE_TYPE"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, CHAR, CHAR, CHAR, VARCHAR, SMALLINT};
        return new ListRecordSet(null, "system", "procedures",
                systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
                                         String columnNamePattern) {
        String[] columns = new String[]{"PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "COLUMN_NAME",
                "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX", "NULLABLE",
                "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
                "IS_NULLABLE", "SPECIFIC_NAME"};

        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, SMALLINT, INTEGER, VARCHAR, INTEGER, SMALLINT,
                SMALLINT, SMALLINT, VARCHAR, VARCHAR, INTEGER, INTEGER, INTEGER, INTEGER, VARCHAR, VARCHAR, VARCHAR};

        return new ListRecordSet(null, "system", "procedure_columns",
                systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) {
        Pattern tableNameRegex = isNullOrEmpty(tableNamePattern) ? null
                : Pattern.compile(tableNamePattern.replace("%", ".*"));

        final Iterable<List<?>> tablesData;
        if (catalog == null) {
            tablesData = clusterInfo.getTables().entrySet().stream()
                    .flatMap(p -> p.getValue().stream().map(t -> asList(p.getKey(), null, t, "TABLE", null, null,
                            null, null, null, null)))
                    .collect(toList());
        } else {
            tablesData = clusterInfo.getTables().getOrDefault(catalog, Collections.emptyList()).stream()
                    .filter(t -> tableNameRegex == null || tableNameRegex.matcher(t).matches())
                    .map(t -> asList(catalog, null, t, "TABLE", null, null, null, null, null, null))
                    .collect(toList());
        }

        String[] columns = new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT",
                "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"};

        int[] sqlTypes = new int[columns.length];
        Arrays.fill(sqlTypes, VARCHAR);
        return new ListRecordSet(null, "system", "tables",
                systemColumns(columns, sqlTypes), tablesData);
    }

    @Override
    public ResultSet getSchemas() {
        return new ListRecordSet(null, "system", "schemas",
                systemColumns(new String[]{"TABLE_SCHEM", "TABLE_CATALOG"}, new int[]{VARCHAR, VARCHAR}),
                clusterInfo.getCatalogs().stream().map(ns -> Arrays.asList("", ns)).collect(toList()));
    }

    @Override
    public ResultSet getCatalogs() {
        return new ListRecordSet(null, "system", "catalogs",
                systemColumns(new String[]{"TABLE_CAT"}, new int[]{VARCHAR}),
                clusterInfo.getCatalogs().stream().map(Collections::singletonList).collect(toList()));
    }

    @Override
    public ResultSet getTableTypes() {
        return new ListRecordSet(null, "system", "table_types",
                systemColumns(new String[]{"TABLE_TYPE"}, new int[]{VARCHAR}),
                singletonList(singletonList("TABLE")));
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
                                String columnNamePattern) throws SQLException {
        logger.info(() -> format("getColumns: %s, %s, %s, %s", catalog, schemaPattern, tableNamePattern,
                columnNamePattern));
        Pattern tableNameRegex = isNullOrEmpty(tableNamePattern) ? null
                : Pattern.compile(tableNamePattern.replace("%", ".*"));

        final List<ResultSetMetaData> resultSetMetaDataList;
        if (catalog == null) {
            resultSetMetaDataList = clusterInfo.getTables().entrySet().stream()
                    .flatMap(p -> p.getValue().stream().map(t -> getMetadata(p.getKey(), t)))
                    .collect(toList());
        } else {
            resultSetMetaDataList = clusterInfo.getTables().getOrDefault(catalog, Collections.emptyList()).stream()
                    .filter(t -> tableNameRegex == null || tableNameRegex.matcher(t).matches())
                    .map(t -> getMetadata(catalog, t))
                    .collect(toList());
        }

        List<List<?>> result = new ArrayList<>();
        for (ResultSetMetaData md : resultSetMetaDataList) {
            int columnCount = md.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                result.add(asList("".equals(tableNamePattern) ? "" : md.getCatalogName(i), null,
                        md.getTableName(1), md.getColumnName(i), md.getColumnType(i), md.getColumnTypeName(i),
                        0, 0, 0, 0, columnNullable, null, null, md.getColumnType(i), 0,
                        md.getColumnType(i) == VARCHAR ? 128 * 1024 : 0, ordinal(md, md.getColumnName(i)),
                        "YES", md.getCatalogName(i), null, md.getColumnTypeName(i), NULL, "NO", "NO"));
            }
        }

        String[] columns = new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
                "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE",
                "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
                "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT",
                "IS_GENERATEDCOLUMN"};

        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, INTEGER, VARCHAR, INTEGER, SMALLINT, INTEGER,
                INTEGER, INTEGER, VARCHAR, VARCHAR, INTEGER, INTEGER, INTEGER, INTEGER, VARCHAR, VARCHAR, VARCHAR,
                VARCHAR, SMALLINT, VARCHAR, VARCHAR};

        return new ListRecordSet(null, "system", "columns",
                systemColumns(columns, sqlTypes), result);
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) {
        String[] columns = new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "GRANTOR", "GRANTEE",
                "PRIVILEGE", "IS_GRANTABLE"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR};
        return new ListRecordSet(null, "system", "column_privileges",
                systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) {
        String[] columns = new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE",
                "IS_GRANTABLE"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR};
        return new ListRecordSet(null, "system", "table_privileges",
                systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) {
        String[] columns = new String[]{"SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE",
                "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"};
        int[] sqlTypes = new int[]{SMALLINT, VARCHAR, INTEGER, VARCHAR, INTEGER, INTEGER, SMALLINT, SMALLINT};
        return new ListRecordSet(null, "system", "best_row_identifier",
                systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) {
        String[] columns = new String[]{"SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE",
                "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"};
        int[] sqlTypes = new int[]{SMALLINT, VARCHAR, INTEGER, VARCHAR, INTEGER, INTEGER, SMALLINT, SMALLINT};
        return new ListRecordSet(null, "system", "version_columns",
                systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) {
        final Iterable<List<?>> tablesData;
        if (catalog == null) {
            tablesData = clusterInfo.getTables().entrySet().stream()
                    .flatMap(p -> p.getValue().stream().map(t ->
                            asList(p.getKey(), null, t, PRIMARY_KEY_COLUMN_NAME, 1, PRIMARY_KEY_COLUMN_NAME)))
                    .collect(toList());
        } else {
            tablesData = clusterInfo.getTables().getOrDefault(catalog, Collections.emptyList()).stream()
                    .filter(t -> table == null || table.equals(t))
                    .map(t -> asList(catalog, null, t, PRIMARY_KEY_COLUMN_NAME, 1, PRIMARY_KEY_COLUMN_NAME))
                    .collect(toList());
        }

        String[] columns = new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, SMALLINT, VARCHAR};
        return new ListRecordSet(null, "system", "primary_keys",
                systemColumns(columns, sqlTypes), tablesData);
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) {
        String[] columns = new String[]{"PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
                "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE",
                "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR,
                SMALLINT, SMALLINT, SMALLINT, VARCHAR, VARCHAR, SMALLINT};
        return new ListRecordSet(null, "system", "imported_keys",
                systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) {
        String[] columns = new String[]{"PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
                "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE",
                "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR,
                SMALLINT, SMALLINT, SMALLINT, VARCHAR, VARCHAR, SMALLINT};
        return new ListRecordSet(null, "system", "exported_keys",
                systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                       String foreignCatalog, String foreignSchema, String foreignTable) {
        String[] columns = new String[]{"PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
                "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE",
                "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, SMALLINT,
                SMALLINT, SMALLINT, VARCHAR, VARCHAR, SMALLINT};
        return new ListRecordSet(null, "system", "cross_references",
                systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getTypeInfo() {
        String[] columns = new String[]{
                "TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX", "LITERAL_SUFFIX", "CREATE_PARAMS", "NULLABLE",
                "CASE_SENSITIVE", "SEARCHABLE", "UNSIGNED_ATTRIBUTE", "FIXED_PREC_SCALE", "AUTO_INCREMENT",
                "LOCAL_TYPE_NAME", "MINIMUM_SCALE", "MAXIMUM_SCALE", "SQL_DATA_TYPE", "SQL_DATETIME_SUB",
                "NUM_PREC_RADIX",
        };

        Iterable<List<?>> data = asList(
                asList("VARCHAR", VARCHAR, 65535, "'", "'",
                        "(M) [CHARACTER SET charset_name] [COLLATE collation_name]", (short) typeNullable,
                        true, (short) typeSearchable, false, false, false, "string",
                        (short) 0, (short) 0, VARCHAR, 0, 10
                ),
                asList("INT", INTEGER, 31, "", "", "[(M)] [UNSIGNED] [ZEROFILL]", (short) typeNullable,
                        true, (short) typeSearchable, false, false, false, "integer",
                        (short) 0, (short) 0, INTEGER, 0, 10
                ),
                asList("DOUBLE", DOUBLE, 22, "", "", "[(M,D)] [UNSIGNED] [ZEROFILL]", (short) typeNullable,
                        true, (short) typeSearchable, false, false, false, "double",
                        (short) 0, (short) 0, DOUBLE, 0, 10
                ),
                asList("BLOB", BLOB, 65535, "", "", "[(M)]", (short) typeNullable,
                        true, (short) typeSearchable, false, false, false, "bytes",
                        (short) 0, (short) 0, BLOB, 0, 10
                ),
                asList("LIST", ARRAY, 0, "", "", "[(M)]", (short) typeNullable,
                        true, (short) typeSearchable, false, false, false, "bytes",
                        (short) 0, (short) 0, ARRAY, 0, 10
                ),
                asList("MAP", OTHER, 0, "", "", "[(M)]", (short) typeNullable,
                        true, (short) typeSearchable, false, false, false, "bytes",
                        (short) 0, (short) 0, OTHER, 0, 10
                ),
                asList("JAVA_OBJECT", JAVA_OBJECT, 0, "", "", "[(M)]", (short) typeNullable,
                        true, (short) typeSearchable, false, false, false, "bytes",
                        (short) 0, (short) 0, JAVA_OBJECT, 0, 10
                )
        );
        return new ListRecordSet(null, "system", "table_info", systemColumns(columns), data);
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) {
        logger.info(() -> format("getIndexInfo: %s, %s, %s", catalog, schema, table));
        initCatalogIndexes();
        Stream<AerospikeSecondaryIndex> secondaryIndexStream;
        if (catalog == null) {
            secondaryIndexStream = catalogIndexes.values().stream().flatMap(Collection::stream);
        } else {
            secondaryIndexStream = catalogIndexes.getOrDefault(catalog, Collections.emptyList()).stream()
                    .filter(index -> index.getNamespace().equals(catalog));
        }
        final Iterable<List<?>> indexData = secondaryIndexStream
                .filter(index -> index.getSet().equals(table))
                .map(this::indexInfoAsList)
                .collect(Collectors.toList());

        String[] columns = new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER",
                "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC", "CARDINALITY",
                "PAGES", "FILTER_CONDITION"};

        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, TINYINT, VARCHAR, VARCHAR, SMALLINT, SMALLINT, VARCHAR,
                VARCHAR, BIGINT, BIGINT, VARCHAR};

        return new ListRecordSet(null, "system", "index_info",
                systemColumns(columns, sqlTypes), indexData);
    }

    @Override
    public boolean supportsResultSetType(int type) {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() {
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) {
        String[] columns = new String[]{"TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME", "DATA_TYPE",
                "REMARKS", "BASE_TYPE"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, INTEGER, VARCHAR, SMALLINT};
        return new ListRecordSet(null, "system", "udt",
                systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) {
        List<List<?>> typeNames = Stream.of("List", "Map", "GeoJSON")
                .map(typeName -> asList(catalog, null, typeName, null, null, Object.class.getName()))
                .collect(toList());

        String[] columns = new String[]{"TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SUPERTYPE_CAT", "SUPERTYPE_SCHEM",
                "SUPERTYPE_NAME"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR};

        return new ListRecordSet(null, "system", "super_types",
                systemColumns(columns, sqlTypes), typeNames);
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) {
        String[] columns = new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "SUPERTABLE_NAME"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR};

        return new ListRecordSet(null, "system", "super_tables",
                systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
                                   String attributeNamePattern) {
        String[] columns = new String[]{"TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "ATTR_NAME", "DATA_TYPE",
                "ATTR_TYPE_NAME", "ATTR_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS",
                "ATTR_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
                "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE"};

        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, INTEGER, VARCHAR, INTEGER, INTEGER, INTEGER,
                INTEGER, VARCHAR, VARCHAR, INTEGER, INTEGER, INTEGER, INTEGER, VARCHAR, VARCHAR, VARCHAR, VARCHAR,
                SMALLINT};

        return new ListRecordSet(null, "system", "attributes",
                systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT == holdability;
    }

    @Override
    public int getResultSetHoldability() {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() {
        return parseVersion(getDatabaseProductVersion().split("\\.")[0]);
    }

    @Override
    public int getDatabaseMinorVersion() {
        String[] fragments = getDatabaseProductVersion().split("\\.");
        return parseVersion(fragments[fragments.length - 1]);
    }

    private int parseVersion(String str) {
        try {
            return Integer.parseInt(str);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public int getJDBCMajorVersion() {
        return JDBC_MAJOR_VERSION;
    }

    @Override
    public int getJDBCMinorVersion() {
        return JDBC_MINOR_VERSION;
    }

    @Override
    public int getSQLStateType() {
        return sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy() {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() {
        return RowIdLifetime.ROWID_VALID_FOREVER;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) {
        String[] columns = new String[]{"TABLE_SCHEM", "TABLE_CATALOG"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR};
        return new ListRecordSet(null, "system", "schemas",
                systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() {
        String[] columns = new String[]{"NAME", "MAX_LEN", "DEFAULT_VALUE", "DESCRIPTION"};
        int[] sqlTypes = new int[]{VARCHAR, INTEGER, VARCHAR, VARCHAR};
        return new ListRecordSet(null, "system", "client_info_properties",
                systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) {
        String[] columns = new String[]{"FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS",
                "FUNCTION_TYPE", "SPECIFIC_NAME"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, SMALLINT, VARCHAR};
        return new ListRecordSet(null, "system", "functions",
                systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
                                        String columnNamePattern) {
        String[] columns = new String[]{"FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "COLUMN_NAME", "COLUMN_TYPE",
                "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS",
                "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME"};

        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, SMALLINT, INTEGER, VARCHAR, INTEGER, INTEGER,
                SMALLINT, SMALLINT, SMALLINT, VARCHAR, INTEGER, INTEGER, VARCHAR, VARCHAR};

        return new ListRecordSet(null, "system", "function_columns",
                systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
                                      String columnNamePattern) {
        String[] columns = new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
                "COLUMN_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "COLUMN_USAGE", "REMARKS", "CHAR_OCTET_LENGTH",
                "IS_NULLABLE"};

        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, INTEGER, INTEGER, INTEGER, INTEGER, VARCHAR,
                VARCHAR, INTEGER, VARCHAR};

        return new ListRecordSet(null, "system", "pseudo_columns",
                systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public boolean generatedKeyAlwaysReturned() {
        return false;
    }

    private List<DataColumn> systemColumns(String[] names) {
        return range(0, names.length).boxed()
                .map(i -> new DataColumn("system", null, names[i], names[i]))
                .collect(toList());
    }

    private List<DataColumn> systemColumns(String[] names, int[] types) {
        return range(0, names.length).boxed()
                .map(i -> new DataColumn("system", null, names[i], names[i]).withType(types[i]))
                .collect(toList());
    }

    private int ordinal(ResultSetMetaData md, String columnName) {
        int ordinal = 0;
        try {
            int columnCount = md.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                if (columnName.equals(md.getColumnName(i))) {
                    ordinal = i;
                    break;
                }
            }
        } catch (SQLException e) {
            logger.severe(() -> format("Exception in ordinal, columnName: %s", columnName));
        }
        return ordinal;
    }

    private ResultSetMetaData getMetadata(String namespace, String table) {
        final String key = format("%s.%s", namespace, table);
        try {
            return resultSetMetaDataCache.get(key, () -> {
                try (Statement statement = connection.createStatement()) {
                    String query = format("SELECT * FROM \"%s.%s\" LIMIT 1", namespace, table);
                    return statement.executeQuery(query).getMetaData();
                } catch (SQLException e) {
                    logger.severe(() -> format("Exception in getMetadata, namespace: %s, table: %s", namespace, table));
                    throw new IllegalArgumentException(e);
                }
            });
        } catch (ExecutionException e) {
            throw new IllegalArgumentException(e.getCause());
        }
    }

    public List<Object> indexInfoAsList(AerospikeSecondaryIndex index) {
        return asList(
                index.getNamespace(),       // TABLE_CAT
                null,                       // TABLE_SCHEM
                index.getSet(),             // TABLE_NAME
                0,                          // NON_UNIQUE
                null,                       // INDEX_QUALIFIER
                index.getIndexName(),       // INDEX_NAME
                tableIndexClustered,        // TYPE
                ordinal(getMetadata(index.getNamespace(), index.getSet()), index.getBinName()), // ORDINAL_POSITION
                index.getBinName(),         // COLUMN_NAME
                null,                       // ASC_OR_DESC
                index.getBinValuesRatio(),  // CARDINALITY
                0,                          // PAGES
                null                        // FILTER_CONDITION
        );
    }
}
