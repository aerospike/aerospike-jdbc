package com.aerospike.jdbc;

import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.query.AerospikeQueryParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.prestosql.sql.SqlFormatter;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Statement;
import org.testng.annotations.Test;

import static com.google.common.base.Strings.repeat;
import static io.prestosql.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static io.prestosql.sql.testing.TreeAssertions.assertFormattedSql;

public class StatementBuilderTest {

    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testStatementBuilder() {
        printStatement("select distinct count(*) as c from catalog.db.foo");
        printStatement("select * from foo order by id, name, phone offset 5 limit 10");
        printStatement("select id, name from foo where id=3 and name like \"%abc%\" or name is not null" +
                " or not name = \"foo\"order by name");
        printStatement("insert into foo (id, name) values (1, 'bar')");
    }

    private static void printStatement(String sql) {
        System.out.println(sql.trim());

        ParsingOptions parsingOptions = new ParsingOptions(AS_DOUBLE);
        Statement statement = SQL_PARSER.createStatement(sql, parsingOptions);

        System.out.println(statement.toString());
        System.out.println(SqlFormatter.formatSql(statement));

        try {
            AerospikeQuery query = AerospikeQueryParser.parseSql(statement);
            System.out.println(mapper.writeValueAsString(query));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        assertFormattedSql(SQL_PARSER, statement);

        System.out.println(repeat("=", 64));
    }
}
