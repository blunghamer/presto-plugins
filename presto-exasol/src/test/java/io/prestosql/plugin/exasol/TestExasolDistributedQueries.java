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

import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.AbstractTestDistributedQueries;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.JdbcSqlExecutor;
import io.prestosql.testing.sql.TestTable;
import io.prestosql.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static io.prestosql.plugin.exasol.ExasolQueryRunner.createExasolQueryRunner;

@Test
public class TestExasolDistributedQueries
        extends AbstractTestDistributedQueries
{
    private TestingExasolServer exasolServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.exasolServer = new TestingExasolServer();
        return createExasolQueryRunner(
                exasolServer,
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        // caching here speeds up tests highly, caching is not used in smoke tests
                        .put("metadata.cache-ttl", "10m")
                        .put("metadata.cache-missing", "true")
                        .build(),
                TpchTable.getTables());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        if (exasolServer != null) {
            exasolServer.close();
        }
    }

    @Override
    protected boolean supportsViews()
    {
        return true;
    }

    @Override
    protected boolean supportsArrays()
    {
        return false;
    }

    @Override
    protected boolean supportsCommentOnColumn()
    {
        return true;
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                new JdbcSqlExecutor(exasolServer.getJdbcUrl()),
                "tpch.table",
                "(" +
                "col_required BIGINT NOT NULL," +
                "col_nullable BIGINT," +
                "col_default BIGINT DEFAULT 43," +
                "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                "col_required2 BIGINT NOT NULL" +
                ")");
    }

    /*
    @Override
    public void testDelete()
    {
        // delete is not supported
    }
    */

    @Test
    public void testInformationSchemaFiltering()
    {
        assertQuery(
                "SELECT table_name FROM sys.exa_all_tables WHERE table_name = 'ORDERS' LIMIT 1",
                "SELECT 'ORDERS' table_name");
        assertQuery(
                "SELECT column_table as table_name FROM sys.exa_all_columns WHERE COLUMN_TYPE = 'DECIMAL(36,0)' AND column_table = 'CUSTOMER' and column_name = 'CUSTKEY' LIMIT 1;",
                "SELECT 'CUSTOMER' table_name");
    }

    // PostgreSQL specific tests should normally go in TestPostgreSqlIntegrationSmokeTest
}
