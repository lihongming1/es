package com.espro.jest.controller;

import com.espro.jest.utils.EsExt;
import com.espro.jest.utils.EsTool;
import com.espro.jest.utils.EsUtils;
import com.espro.jest.utils.JsonUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import io.searchbox.core.Get;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.indices.mapping.PutMapping;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@RestController
@RequestMapping("/test")
public class TestController {

    @Autowired
    private JestClient jestClient;

    @RequestMapping(value = "/test", method = RequestMethod.GET)
    public String test() throws Exception {
//        PutMapping putMapping = EsUtils.putMapping();
//        JestResult jr = jestClient.execute(putMapping);
//        System.out.println(jr.isSucceeded());

//        Map<String, String> map = new HashMap<>();
//        map.put("id", "2");
//        map.put("title", "Java设计模式之装饰模式");
//        map.put("content", "在不必改变原类文件和使用继承的情况下，动态地扩展一个对象的功能。");
//        map.put("postdate", "2018-02-03 14:38:00");
//        map.put("url", "csdn.net/79239072");
//
//        Map<String, String> map2 = new HashMap<>();
//        map2.put("id", "3");
//        map2.put("title", "ES设计模式之装饰模式");
//        map2.put("content", "在不ES情况下，动态地扩展一个对象的功能。");
//        map2.put("postdate", "2018-12-03 14:38:00");
//        map2.put("url", "csdn.net/SSSSSSSS");
//
//        final List<String> list = new ArrayList<>();
//        list.add(JsonUtils.toJson(map));
//        list.add(JsonUtils.toJson(map2));
//
//        String resultJson = EsTool.batchSaveUpdateIndex(jestClient, "index1", "blog", list);

        String resultJson = EsUtils.deleteDoc(jestClient, "index1", "blog","1s");

        return resultJson;
    }

}
