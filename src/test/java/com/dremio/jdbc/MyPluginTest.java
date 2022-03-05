package com.dremio.jdbc;

import com.dremio.BaseTestQuery;
import com.dremio.TestResult;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.jdbc.conf.QuestDBConf;
import com.dremio.options.OptionValue;
import com.dremio.service.namespace.source.proto.SourceConfig;
import org.junit.Before;
import org.junit.Test;

public class MyPluginTest extends BaseTestQuery {
    private QuestDBConf questDBConf;

    @Before
    public  void initSource(){
        getSabotContext().getOptionManager().setOption(OptionValue.createLong(OptionValue.OptionType.SYSTEM, ExecConstants.ELASTIC_ACTION_RETRIES, 3));
        SourceConfig sc = new SourceConfig();
        sc.setName("cratedb");
        questDBConf = new QuestDBConf();
        questDBConf.host="127.0.0.1";
        questDBConf.port=5433;
        questDBConf.username="crate";
        sc.setConnectionConf(questDBConf);
        sc.setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY);
        getSabotContext().getCatalogService().createSourceIfMissingWithThrow(sc);
    }

    @Test
    public  void test() throws Exception {
        String query  = "select * from cratedb.doc.demoapp";
        TestResult testResult=  testBuilder()
                .sqlQuery(query)
                .unOrdered()
                .baselineColumns("id", "name")
                .baselineValues(null,    null)
                .go();
    }


}
