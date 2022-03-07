package com.dremio.jdbc;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.dremio.*;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.jdbc.conf.QuestDBConf;
import com.dremio.options.OptionValue;
import com.dremio.service.namespace.source.proto.SourceConfig;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class MyPluginTest2 extends BaseTestQuery2 {

    @Before
    public void setUp(){
        LoggerContext context = (LoggerContext)LoggerFactory.getILoggerFactory();
        Logger root = context.getLogger("root");
        root.setLevel(Level.ALL);
    }

    @Before
    public  void initSource(){
        getSabotContext().getOptionManager().setOption(OptionValue.createLong(
                OptionValue.OptionType.SYSTEM, ExecConstants.ELASTIC_ACTION_RETRIES, 3));
        SourceConfig sc = new SourceConfig();
        sc.setName("questdb");
        QuestDBConf questDBConf = new QuestDBConf();
        questDBConf.host="127.0.0.1";
        questDBConf.port=8812;
        questDBConf.user ="admin";
        questDBConf.password ="quest";

        sc.setConnectionConf(questDBConf);
        sc.setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY);
        getSabotContext().getCatalogService().createSourceIfMissingWithThrow(sc);
    }

    @Test
    public  void test() throws Exception {
        String query  = "select id,name from questdb.demoapp limit 1";
        TestResult testResult=  testBuilder()
                .sqlQuery(query)
                .unOrdered()
                .baselineColumns("id", "name")
                .baselineValues(1, "a") // expect value
                .go();
    }

}
