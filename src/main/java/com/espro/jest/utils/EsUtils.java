package com.espro.jest.utils;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.*;
import io.searchbox.indices.mapping.PutMapping;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class EsUtils {

    public static PutMapping putMapping() throws Exception {

        XContentBuilder builder = jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("id")
                .field("type", "long")
                .endObject()
                .startObject("title")
                .field("type", "text")
                .field("analyzer", "ik_max_word")
                .field("search_analyzer", "ik_max_word")
                .field("boost", 2)
                .endObject()
                .startObject("content")
                .field("type", "text")
                .field("analyzer", "ik_max_word")
                .field("search_analyzer", "ik_max_word")
                .endObject()
                .startObject("postdate")
                .field("type", "date")
                .field("format", "yyyy-MM-dd HH:mm:ss")
                .endObject()
                .startObject("url")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject();

        String json = Strings.toString(builder);

        PutMapping putMapping = new PutMapping.Builder("index1", "blog", json).build();

        return putMapping;
    }

    /**
     * 创建Mapping
     * @param jestClient
     * @param indexName
     * @param indexType
     * @param xContentBuilder
     * @return
     * @throws Exception
     */
    public static String putMapping(JestClient jestClient, String indexName, String indexType, XContentBuilder xContentBuilder) throws Exception{
        String json = Strings.toString(xContentBuilder);
        PutMapping putMapping = new PutMapping.Builder(indexName, indexType, json).build();
        JestResult result = jestClient.execute(putMapping);
        return result.getJsonString();
    }

    /**
     * 新增或更新索引
     * @param jestClient
     * @param indexName
     * @param indexType
     * @param indexId
     * @param source
     * @return
     * @throws Exception
     */
    public static String saveUpdateIndex(JestClient jestClient, String indexName, String indexType, String indexId, String source) throws Exception {
        Index.Builder builder = new Index.Builder(source);
        builder.id(indexId);
        builder.refresh(true);
        Index index = builder.index(indexName).type(indexType).build();
        JestResult result = jestClient.execute(index);
        return result.getJsonString();
    }

    /**
     * 批量- 新增或更新索引
     * @param jestClient
     * @param indexName
     * @param indexType
     * @param sources
     * @return
     * @throws Exception
     */
    public static String batchSaveUpdateIndex(JestClient jestClient, String indexName, String indexType, List<String> sources, EsExt ext) throws Exception {
        if(ext != null){
            // 执行扩展功能
            return ext.ext();
        }else{
            Bulk.Builder bulkBuilder = new Bulk.Builder().defaultIndex(indexName).defaultType(indexType);
            //循环构造批量数据
            for (String source : sources) {
                Index index = new Index.Builder(source).build();
                bulkBuilder.addAction(index);
            }
            JestResult result = jestClient.execute(bulkBuilder.build());
            return result.getJsonString();
        }

    }

    /**
     * 获取文档
     * @param jestClient
     * @param indexName
     * @param indexType
     * @param indexId
     * @return
     * @throws Exception
     */
    public static String getDoc(JestClient jestClient, String indexName, String indexType, String indexId) throws Exception{
        Get get = new Get.Builder(indexName, indexId).type(indexType).build();
        JestResult result = jestClient.execute(get);
        return result.getJsonString();
    }

    /**
     * 删除文档
     * @param jestClient
     * @param indexName
     * @param indexType
     * @param indexId
     * @return
     * @throws Exception
     */
    public static String deleteDoc(JestClient jestClient, String indexName, String indexType, String indexId) throws Exception{
        Delete delete = new Delete.Builder(indexId).index(indexName).type(indexType).build();
        JestResult result = jestClient.execute(delete);
        return result.getJsonString();
    }

}
