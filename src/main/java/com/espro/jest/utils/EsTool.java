package com.espro.jest.utils;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.util.List;

public class EsTool {

    public static String batchSaveUpdateIndex(final JestClient jestClient, final String indexName, final String indexType, final List<String> sources) throws Exception {
        String resultJson = EsUtils.batchSaveUpdateIndex(jestClient, indexName, indexType, sources, new EsExt() {
            @Override
            public String ext() throws Exception {
                Bulk.Builder bulkBuilder = new Bulk.Builder().defaultIndex(indexName).defaultType(indexType);
                //循环构造批量数据
                for (String source : sources) {
                    Index index = new Index.Builder(source).build();
                    bulkBuilder.addAction(index);
                }
                JestResult result = jestClient.execute(bulkBuilder.build());
                return result.getJsonString();
            }
        });
        return resultJson;
    }

}
