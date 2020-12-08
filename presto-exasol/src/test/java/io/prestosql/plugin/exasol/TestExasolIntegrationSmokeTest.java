/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.exasol;

import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.UUID;

import static io.prestosql.tpch.TpchTable.CUSTOMER;
import static io.prestosql.tpch.TpchTable.NATION;
import static io.prestosql.tpch.TpchTable.ORDERS;
import static io.prestosql.tpch.TpchTable.REGION;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class TestExasolIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    protected TestingExasolServer exasolServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.exasolServer = new TestingExasolServer();
        return ExasolQueryRunner.createExasolQueryRunner(this.exasolServer, CUSTOMER, NATION, ORDERS, REGION);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        if (exasolServer != null) {
            exasolServer.close();
        }
    }

    @Test
    public void testDropTable()
    {
        assertUpdate("CREATE TABLE test_drop AS SELECT 123 x", 1);
        assertTrue(getQueryRunner().tableExists(getSession(), "test_drop"));

        assertUpdate("DROP TABLE test_drop");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_drop"));
    }

    @Test
    public void testInsert()
            throws Exception
    {
        execute("CREATE TABLE tpch.test_insert (x bigint, y varchar(100))");
        assertUpdate("INSERT INTO test_insert VALUES (123, 'test')", 1);
        assertQuery("SELECT * FROM test_insert", "SELECT 123 x, 'test' y");
        assertUpdate("DROP TABLE test_insert");
    }

    @Test
    public void testInsertInPresenceOfNotSupportedColumn()
            throws Exception
    {
        execute("CREATE TABLE tpch.test_insert_not_supported_column_present(x bigint, y decimal(50,0), z varchar(10))");
        // Check that column y is not supported.
        assertQuery("SELECT column_name FROM sys.exa_all_columns WHERE column_name = 'test_insert_not_supported_column_present'", "VALUES 'x', 'z'");
        assertUpdate("INSERT INTO test_insert_not_supported_column_present (x, z) VALUES (123, 'test')", 1);
        assertQuery("SELECT x, z FROM test_insert_not_supported_column_present", "SELECT 123, 'test'");
        assertUpdate("DROP TABLE test_insert_not_supported_column_present");
    }

    @Test
    public void testViews()
            throws Exception
    {
        execute("CREATE OR REPLACE VIEW tpch.test_view AS SELECT * FROM tpch.orders");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_view"));
        assertQuery("SELECT orderkey FROM test_view", "SELECT orderkey FROM orders");
        execute("DROP VIEW IF EXISTS tpch.test_view");
    }

    /*
    @Test
    public void testSystemTable()
    {
        assertThat(computeActual("SHOW TABLES FROM pg_catalog").getOnlyColumnAsSet())
                .contains("pg_tables", "pg_views", "pg_type", "pg_index");
        // SYSTEM TABLE
        assertThat(computeActual("SELECT typname FROM pg_catalog.pg_type").getOnlyColumnAsSet())
                .contains("char", "text");
        // SYSTEM VIEW
        assertThat(computeActual("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'tpch'").getOnlyColumn())
                .contains("orders");
    }
    */

    @Test
    public void testTableWithNoSupportedColumns()
            throws Exception
    {
        String unsupportedDataType = "array";
        String supportedDataType = "varchar(5)";

        try (AutoCloseable ignore1 = withTable("tpch.no_supported_columns", format("(c %s)", unsupportedDataType));
                AutoCloseable ignore2 = withTable("tpch.supported_columns", format("(good %s)", supportedDataType));
                AutoCloseable ignore3 = withTable("tpch.no_columns", "()")) {
            assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet()).contains("orders", "no_supported_columns", "supported_columns", "no_columns");

            assertQueryFails("SELECT c FROM no_supported_columns", "Table 'tpch.no_supported_columns' not found");
            assertQueryFails("SELECT * FROM no_supported_columns", "Table 'tpch.no_supported_columns' not found");
            assertQueryFails("SELECT 'a' FROM no_supported_columns", "Table 'tpch.no_supported_columns' not found");

            assertQueryFails("SELECT c FROM no_columns", "Table 'tpch.no_columns' not found");
            assertQueryFails("SELECT * FROM no_columns", "Table 'tpch.no_columns' not found");
            assertQueryFails("SELECT 'a' FROM no_columns", "Table 'tpch.no_columns' not found");

            assertQueryFails("SELECT c FROM non_existent", ".* Table .*tpch.non_existent.* does not exist");
            assertQueryFails("SELECT * FROM non_existent", ".* Table .*tpch.non_existent.* does not exist");
            assertQueryFails("SELECT 'a' FROM non_existent", ".* Table .*tpch.non_existent.* does not exist");

            assertQuery("SHOW COLUMNS FROM no_supported_columns", "SELECT 'nothing' WHERE false");
            assertQuery("SHOW COLUMNS FROM no_columns", "SELECT 'nothing' WHERE false");

            // Other tables should be visible in SHOW TABLES (the no_supported_columns might be included or might be not) and sys.exa_all_tables
            assertThat(computeActual("SHOW TABLES").getOnlyColumn())
                    .contains("orders", "no_supported_columns", "supported_columns", "no_columns");
            assertThat(computeActual("SELECT table_name FROM sys.exa_all_tables WHERE table_schema = 'tpch'").getOnlyColumn())
                    .contains("orders", "no_supported_columns", "supported_columns", "no_columns");

            // Other tables should be introspectable with SHOW COLUMNS and sys.exa_all_columns
            assertQuery("SHOW COLUMNS FROM supported_columns", "VALUES ('good', 'varchar(5)', '', '')");

            // Listing columns in all tables should not fail due to tables with no columns
            computeActual("SELECT column_name FROM sys.exa_all_columns WHERE column_schema = 'tpch'");
        }
    }

    @Test
    public void testInsertWithFailureDoesNotLeaveBehindOrphanedTable()
            throws Exception
    {
        String schemaName = format("tmp_schema_%s", UUID.randomUUID().toString().replaceAll("-", ""));
        try (AutoCloseable schema = withSchema(schemaName);
                AutoCloseable table = withTable(format("%s.test_cleanup", schemaName), "(x INTEGER)")) {
            assertQuery(format("SELECT table_name FROM sys.exa_all_tables WHERE table_schema = '%s'", schemaName.toUpperCase(Locale.ROOT)), "VALUES 'test_cleanup'".toUpperCase(Locale.ROOT));
            //execute(format("ALTER TABLE %s.test_cleanup ADD CHECK (x > 0)", schemaName));
            assertQueryFails(format("INSERT INTO %s.test_cleanup (x) VALUES ('uh')", schemaName), "Insert query has mismatched column types.*");
            assertQuery(format("SELECT table_name FROM sys.exa_all_tables WHERE table_schema = '%s'", schemaName.toUpperCase(Locale.ROOT)), "VALUES 'test_cleanup'".toUpperCase(Locale.ROOT));
        }
    }

    @Test
    public void testDecimalPredicatePushdown()
            throws Exception
    {
        // TODO test that that predicate is actually pushed down (here we test only correctness)
        try (AutoCloseable ignoreTable = withTable("tpch.test_decimal_pushdown",
                "(short_decimal decimal(9, 3), long_decimal decimal(30, 10))")) {
            execute("INSERT INTO tpch.test_decimal_pushdown VALUES (123.321, 123456789.987654321)");

            assertQuery("SELECT * FROM tpch.test_decimal_pushdown WHERE short_decimal <= 124",
                    "VALUES (123.321, 123456789.987654321)");
            assertQuery("SELECT * FROM tpch.test_decimal_pushdown WHERE long_decimal <= 123456790",
                    "VALUES (123.321, 123456789.987654321)");
            assertQuery("SELECT * FROM tpch.test_decimal_pushdown WHERE short_decimal <= 123.321",
                    "VALUES (123.321, 123456789.987654321)");
            assertQuery("SELECT * FROM tpch.test_decimal_pushdown WHERE long_decimal <= 123456789.987654321",
                    "VALUES (123.321, 123456789.987654321)");
            assertQuery("SELECT * FROM tpch.test_decimal_pushdown WHERE short_decimal = 123.321",
                    "VALUES (123.321, 123456789.987654321)");
            assertQuery("SELECT * FROM tpch.test_decimal_pushdown WHERE long_decimal = 123456789.987654321",
                    "VALUES (123.321, 123456789.987654321)");
        }
    }

    @Test
    public void testCharPredicatePushdown()
            throws Exception
    {
        // TODO test that that predicate is actually pushed down (here we test only correctness)
        try (AutoCloseable ignoreTable = withTable("tpch.test_char_pushdown",
                "(char_1 char(1), char_5 char(5), char_10 char(10))")) {
            execute("INSERT INTO tpch.test_char_pushdown VALUES" +
                    "('0', '0'    , '0'         )," +
                    "('1', '12345', '1234567890')");

            assertQuery("SELECT * FROM tpch.test_char_pushdown WHERE char_1 = '0' AND char_5 = '0'",
                    "VALUES ('0', '0    ', '0         ')");
            assertQuery("SELECT * FROM tpch.test_char_pushdown WHERE char_5 = CHAR'12345' AND char_10 = '1234567890'",
                    "VALUES ('1', '12345', '1234567890')");
            assertQuery("SELECT * FROM tpch.test_char_pushdown WHERE char_10 = CHAR'0'",
                    "VALUES ('0', '0    ', '0         ')");
        }
    }

    @Test
    public void testCharTrailingSpace()
            throws Exception
    {
        execute("CREATE TABLE tpch.char_trailing_space (x char(10))");
        assertUpdate("INSERT INTO tpch.char_trailing_space VALUES ('test')", 1);

        assertQuery("SELECT * FROM tpch.char_trailing_space WHERE x = cast ('test' as char(4)) ", "VALUES 'test'");
        assertQuery("SELECT * FROM tpch.char_trailing_space WHERE x = cast ('test  ' as char(4))", "VALUES 'test'");
        assertQuery("SELECT * FROM tpch.char_trailing_space WHERE x = cast ('test        ' as char(4))", "VALUES 'test'");

        assertEquals(getQueryRunner().execute("SELECT * FROM tpch.char_trailing_space WHERE x = cast( ' test' as char(5))").getRowCount(), 0);

        assertUpdate("DROP TABLE tpch.char_trailing_space");
    }

    @Test
    public void testInsertIntoNotNullColumn()
    {
        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s.tpch.test_insert_not_null (\n" +
                        "   column_a date,\n" +
                        "   column_b date NOT NULL\n" +
                        ")",
                getSession().getCatalog().get());
        assertUpdate(createTableSql);
        assertEquals(computeScalar("SHOW CREATE TABLE test_insert_not_null"), createTableSql);

        assertQueryFails("INSERT INTO test_insert_not_null (column_a) VALUES (date '2012-12-31')", "(?s).*constraint violation - not null.*");
        assertQueryFails("INSERT INTO test_insert_not_null (column_a, column_b) VALUES (date '2012-12-31', null)", "NULL value not allowed for NOT NULL column: column_b");

        assertUpdate("ALTER TABLE test_insert_not_null ADD COLUMN column_c BIGINT NOT NULL");

        createTableSql = format("" +
                        "CREATE TABLE %s.tpch.test_insert_not_null (\n" +
                        "   column_a date,\n" +
                        "   column_b date NOT NULL,\n" +
                        "   column_c bigint NOT NULL\n" +
                        ")",
                getSession().getCatalog().get());
        assertEquals(computeScalar("SHOW CREATE TABLE test_insert_not_null"), createTableSql);

        assertQueryFails("INSERT INTO test_insert_not_null (column_b) VALUES (date '2012-12-31')", "(?s).*constraint violation - not null.*");
        assertQueryFails("INSERT INTO test_insert_not_null (column_b, column_c) VALUES (date '2012-12-31', null)", "NULL value not allowed for NOT NULL column: column_c");

        assertUpdate("INSERT INTO test_insert_not_null (column_b, column_c) VALUES (date '2012-12-31', 1)", 1);
        assertUpdate("INSERT INTO test_insert_not_null (column_a, column_b, column_c) VALUES (date '2013-01-01', date '2013-01-02', 2)", 1);
        assertQuery(
                "SELECT * FROM test_insert_not_null",
                "VALUES (NULL, CAST('2012-12-31' AS DATE), 1), (CAST('2013-01-01' AS DATE), CAST('2013-01-02' AS DATE), 2)");

        assertUpdate("DROP TABLE test_insert_not_null");
    }

    @Test
    public void testAggregationPushdown()
            throws Exception
    {
        // TODO support aggregation pushdown with GROUPING SETS
        // TODO support aggregation over expressions

        // SELECT DISTINCT

        assertThat(query("SELECT DISTINCT regionkey FROM nation")).isFullyPushedDown();

        assertThat(query("SELECT count(*) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count(nationkey) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count(1) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count() FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, min(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, max(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, avg(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();

        try (AutoCloseable ignoreTable = withTable("tpch.test_aggregation_pushdown", "(short_decimal decimal(9, 3), long_decimal decimal(30, 10))")) {
            execute("INSERT INTO tpch.test_aggregation_pushdown VALUES (100.000, 100000000.000000000)");
            execute("INSERT INTO tpch.test_aggregation_pushdown VALUES (123.321, 123456789.987654321)");

            assertThat(query("SELECT min(short_decimal), min(long_decimal) FROM test_aggregation_pushdown")).isFullyPushedDown();
            assertThat(query("SELECT max(short_decimal), max(long_decimal) FROM test_aggregation_pushdown")).isFullyPushedDown();
            assertThat(query("SELECT sum(short_decimal), sum(long_decimal) FROM test_aggregation_pushdown")).isFullyPushedDown();
            assertThat(query("SELECT avg(short_decimal), avg(long_decimal) FROM test_aggregation_pushdown")).isFullyPushedDown();
        }
    }

    @Test
    public void testColumnComment()
            throws Exception
    {
        try (AutoCloseable ignore = withTable("tpch.test_column_comment",
                "(col1 bigint, col2 bigint, col3 bigint)")) {
            execute("COMMENT ON COLUMN tpch.test_column_comment.col1 IS 'test comment'");
            execute("COMMENT ON COLUMN tpch.test_column_comment.col2 IS ''"); // it will be NULL, Exasol does empty the comment on empty string

            assertQuery(
                    "SELECT column_name, column_comment FROM sys.exa_all_columns WHERE column_schema = 'TPCH' AND column_table = 'TEST_COLUMN_COMMENT'",
                    "VALUES ('COL1', 'test comment'), ('COL2', null), ('COL3', null)");
        }
    }

    private AutoCloseable withSchema(String schema)
            throws Exception
    {
        execute(format("CREATE SCHEMA %s", schema));
        return () -> {
            try {
                execute(format("DROP SCHEMA %s CASCADE", schema));
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private AutoCloseable withTable(String tableName, String tableDefinition)
            throws Exception
    {
        execute(format("CREATE TABLE %s%s", tableName, tableDefinition));
        return () -> {
            try {
                execute(format("DROP TABLE %s CASCADE", tableName));
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private void execute(String sql)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(exasolServer.getJdbcUrl(), exasolServer.getUsername(), exasolServer.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
