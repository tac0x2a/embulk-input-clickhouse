Embulk::JavaPlugin.register_input(
  "clickhouse", "org.embulk.input.clickhouse.ClickhouseInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
