package com.dremio.exec.store.jdbc.conf;

import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.store.jdbc.*;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;
import com.dremio.options.OptionManager;
import com.dremio.security.CredentialsService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import io.protostuff.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

@SourceType(value = "QUESTDB", label = "QUESTDB", uiConfig = "quest-layout.json")
public class QuestDBConf extends AbstractArpConf<QuestDBConf> {
    private static final String ARP_FILENAME = "arp/implementation/quest-arp.yaml";
    private static final String DRIVER = "org.postgresql.Driver";

    static class QuestdbSchemaFetcher extends ArpDialect.ArpSchemaFetcher {
        private static final Logger logger = LoggerFactory.getLogger(QuestdbSchemaFetcher.class);
        private final JdbcPluginConfig config;

        public QuestdbSchemaFetcher(String query, JdbcPluginConfig config) {
            super(query, config);
            this.config = config;
            logger.info("query schema:{} config: {}", query, config);
        }

        @Override
        protected JdbcFetcherProto.CanonicalizeTablePathResponse getDatasetHandleViaGetTables(JdbcFetcherProto.CanonicalizeTablePathRequest request, Connection connection) throws SQLException {
            com.dremio.exec.store.jdbc.JdbcFetcherProto.CanonicalizeTablePathResponse.Builder responseBuilder = JdbcFetcherProto.CanonicalizeTablePathResponse.newBuilder();
            responseBuilder.setTable(request.getTable());
            return responseBuilder.build();
        }

    }

    static class QuestdbDialect extends ArpDialect {
        public QuestdbDialect(ArpYaml yaml) {
            super(yaml);
        }

        @Override
        public ArpSchemaFetcher newSchemaFetcher(JdbcPluginConfig config) {
            String query = String.format("SELECT NULL,NULL,name NME from TABLES where name not in ('telemetry','telemetry_config','%s')", new Object[]{Joiner.on("','").join(config.getHiddenSchemas())});
            return new QuestdbSchemaFetcher(query, config);
        }

        @Override
        public ContainerSupport supportsCatalogs() {
            return ContainerSupport.UNSUPPORTED;
        }

        @Override
        public boolean supportsNestedAggregations() {
            return false;
        }
    }

    @Tag(1)
    @DisplayMetadata(label = "username")
    @NotMetadataImpacting
    public String user = "admin";

    @Tag(2)
    @DisplayMetadata(label = "host")
    public String host;

    @Tag(3)
    @Secret
    @DisplayMetadata(label = "password")
    @NotMetadataImpacting
    public String password = "";

    @Tag(4)
    @Secret
    @DisplayMetadata(label = "database")
    @NotMetadataImpacting
    public String database = "";

    @Tag(5)
    @DisplayMetadata(label = "port")
    @NotMetadataImpacting
    public int port = 5432;

    @Tag(6)
    @DisplayMetadata(label = "Record fetch size")
    @NotMetadataImpacting
    public int fetchSize = 200;


    @Tag(7)
    @DisplayMetadata(
            label = "Maximum idle connections"
    )
    @NotMetadataImpacting
    public int maxIdleConns = 8;

    @Tag(8)
    @DisplayMetadata(
            label = "Connection idle time (s)"
    )
    @NotMetadataImpacting
    public int idleTimeSec = 60;

    @VisibleForTesting
    public String toJdbcConnectionString() {
        checkNotNull(this.user, "Missing username.");
        // format jdbc:postgresql://localhost:8812/qdb
        final String format = String.format("jdbc:postgresql://%s:%d/?sslmode=disable", this.host, this.port);
        return format;
    }

    @Override
    @VisibleForTesting
    public JdbcPluginConfig buildPluginConfig(
            JdbcPluginConfig.Builder configBuilder,
            CredentialsService credentialsService,
            OptionManager optionManager
    ) {

        return configBuilder.withDialect(getDialect())
                .withFetchSize(fetchSize)
                .clearHiddenSchemas()
                .withDatasourceFactory(this::newDataSource)
                .build();
    }

    private CloseableDataSource newDataSource() {
        Properties properties = new Properties();
        properties.setProperty("database",this.database);
        CloseableDataSource dataSource = DataSources.newGenericConnectionPoolDataSource(DRIVER,
                toJdbcConnectionString(), this.user, this.password, properties, DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE, this.maxIdleConns, this.idleTimeSec);
        return dataSource;
    }

    @Override
    public ArpDialect getDialect() {
        return AbstractArpConf.loadArpFile(ARP_FILENAME, QuestdbDialect::new);
    }
}
