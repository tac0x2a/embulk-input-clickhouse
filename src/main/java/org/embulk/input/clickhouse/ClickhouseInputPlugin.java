package org.embulk.input.clickhouse;

import com.google.common.base.Optional;

import net.tac42.clickhouse.settings.ClickHouseConnectionSettings;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.JdbcInputConnection;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.joda.time.DateTimeZone;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class ClickhouseInputPlugin extends AbstractJdbcInputPlugin
{
    public interface ClickHousePluginTask
        extends AbstractJdbcInputPlugin.PluginTask
    {
        @Config("driver_path")
        @ConfigDefault("null")
        public Optional<String> getDriverPath();

        @Config("host")
        public String getHost();

        @Config("port")
        @ConfigDefault("8123")
        public int getPort();

        @Config("user")
        @ConfigDefault("null")
        public Optional<String> getUser();

        @Config("password")
        @ConfigDefault("null")
        public Optional<String> getPassword();

        @Config("database")
        public String getDatabase();

        @Config("buffer_size")
        @ConfigDefault("65536")
        public Optional<Integer> getBufferSize();

        @Config("apache_buffer_size")
        @ConfigDefault("65536")
        public Optional<Integer> getApacheBufferSize();

        @Config("connect_timeout")
        @ConfigDefault("30000")
        public int getConnectTimeout();

        @Config("socket_timeout")
        @ConfigDefault("10000")
        public int getSocketTimeout();

        /**
         * Timeout for data transfer. socketTimeout + dataTransferTimeout is sent to ClickHouse as max_execution_time.
         * ClickHouse rejects request execution if its time exceeds max_execution_time
         */
        @Config("data_transfer_timeout")
        @ConfigDefault("10000")
        public Optional<Integer> getDataTransferTimeout();

        @Config("keep_alive_timeout")
        @ConfigDefault("30000")
        public Optional<Integer> getKeepAliveTimeout();
    }

    @Override
    protected Class<? extends AbstractJdbcInputPlugin.PluginTask> getTaskClass()
    {
        return ClickHousePluginTask.class;
    }

    @Override
    protected JdbcInputConnection newConnection(AbstractJdbcInputPlugin.PluginTask task) throws SQLException
    {
        //final String driverClass = "ru.yandex.clickhouse.ClickHouseDriver";
        final String driverClass = "net.tac42.clickhouse.ClickHouseDriver";

        ClickHousePluginTask t = (ClickHousePluginTask) task;

        loadDriver(driverClass, t.getDriverPath());

        Properties props = new Properties();
        if (t.getUser().isPresent()) {
            props.setProperty("user", t.getUser().get());
        }
        if (t.getPassword().isPresent()) {
            props.setProperty("password", t.getPassword().get());
        }

        // ClickHouse Connection Options
        if (t.getApacheBufferSize().isPresent()) {
            props.setProperty(ClickHouseConnectionSettings.APACHE_BUFFER_SIZE.getKey(), String.valueOf(t.getApacheBufferSize().get())); // byte?
        }
        if (t.getBufferSize().isPresent()) {
            props.setProperty(ClickHouseConnectionSettings.BUFFER_SIZE.getKey(), String.valueOf(t.getBufferSize().get())); // byte?
        }
        if (t.getDataTransferTimeout().isPresent()) {
            props.setProperty(ClickHouseConnectionSettings.DATA_TRANSFER_TIMEOUT.getKey(), String.valueOf(t.getDataTransferTimeout().get())); // seconds
        }
        if (t.getKeepAliveTimeout().isPresent()) {
            props.setProperty(ClickHouseConnectionSettings.KEEP_ALIVE_TIMEOUT.getKey(), String.valueOf(t.getKeepAliveTimeout().get())); // seconds
        }

        props.setProperty(ClickHouseConnectionSettings.SOCKET_TIMEOUT.getKey(), String.valueOf(t.getSocketTimeout())); // seconds
        props.setProperty(ClickHouseConnectionSettings.CONNECTION_TIMEOUT.getKey(), String.valueOf(t.getConnectTimeout())); // seconds
        props.putAll(t.getOptions());

        final String url = String.format("jdbc:clickhouse://%s:%d/%s", t.getHost(), t.getPort(), t.getDatabase());

        logConnectionProperties(url, props);

        Connection con = DriverManager.getConnection(url, props);
        try {
            ClickHouseInputConnection c = new ClickHouseInputConnection(con, null);
            con = null;
            return c;
        }
        finally {
            if (con != null) {
                con.close();
            }
        }
    }

    @Override
    protected ColumnGetterFactory newColumnGetterFactory(PageBuilder pageBuilder, DateTimeZone dateTimeZone)
    {
        return new ClickHouseColumnGetterFactory(pageBuilder, dateTimeZone);
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, InputPlugin.Control control)
    {
        // TBD
        return super.transaction(config, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, Schema schema, int taskCount, InputPlugin.Control control)
    {
        // TBD
        return super.resume(taskSource, schema, taskCount, control);
    }

    @Override
    public void cleanup(TaskSource taskSource, Schema schema, int taskCount, List<TaskReport> successTaskReports)
    {
        //TBD
        super.cleanup(taskSource, schema, taskCount, successTaskReports);
    }

    @Override
    public TaskReport run(TaskSource taskSource, Schema schema, int taskIndex, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        //TBD
        return super.run(taskSource, schema, taskIndex, output);
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        //TBD
        return super.guess(config);
    }
}
