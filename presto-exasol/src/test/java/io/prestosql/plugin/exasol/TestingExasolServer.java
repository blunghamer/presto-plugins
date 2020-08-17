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

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.String.format;

public class TestingExasolServer
        implements Closeable
{
    private static final String USER = "sys";
    private static final String PASSWORD = "exasol";
    // exasol does not support multiple databases
    private static final String DATABASE = "tpch";
    private static final String IPADDRESS = "127.0.0.1";
    private static final String PORT = "8899";

    public TestingExasolServer()
    {
        // docker run --name exasoldb -p 127.0.0.1:8899:8888 --detach --privileged --stop-timeout 120 exasol/docker-db:latest-6.1
        // do nothing, we run this externally at the moment
        /*
        dockerContainer = new PostgreSQLContainer("postgres:10.3")
                .withDatabaseName(DATABASE)
                .withUsername(USER)
                .withPassword(PASSWORD);
        dockerContainer.start();
        */
    }

    public String getUsername()
    {
        return USER;
    }

    public String getPassword()
    {
        return PASSWORD;
    }

    public void execute(String sql)
    {
        execute(sql, USER, PASSWORD);
    }

    public void execute(String sql, String user, String password)
    {
        try (Connection connection = DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public String getJdbcUrl()
    {
        return format("jdbc:exa:%s:%s", IPADDRESS, PORT);
    }

    @Override
    public void close()
    {
        //dockerContainer.close();
    }
}
