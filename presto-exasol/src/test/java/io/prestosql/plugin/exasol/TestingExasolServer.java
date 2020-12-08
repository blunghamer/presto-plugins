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

import com.exasol.containers.ExasolContainer;
import io.airlift.log.Logger;
import io.airlift.log.Logging;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.String.format;

public class TestingExasolServer
        implements Closeable
{
    Logger log = Logger.get(TestingExasolServer.class);

    private ExasolContainer<? extends ExasolContainer<?>> dockerContainer;

    public TestingExasolServer()
    {
        dockerContainer = new ExasolContainer<>();
        dockerContainer.withReuse(true);
        dockerContainer.start();

        Logging.initialize();
    }

    public String getUsername()
    {
        log.info("======== username ======== %s", dockerContainer.getUsername());
        return dockerContainer.getUsername();
    }

    public String getPassword()
    {
        log.info("======== password ======== %s", dockerContainer.getPassword());
        return dockerContainer.getPassword();
    }

    public void execute(String sql)
    {
        execute(sql, getUsername(), getPassword());
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
        return format("jdbc:exa:%s", dockerContainer.getExaConnectionAddress());
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
