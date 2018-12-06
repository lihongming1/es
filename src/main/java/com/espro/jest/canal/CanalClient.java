package com.espro.jest.canal;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.espro.jest.utils.EsUtils;
import com.espro.jest.utils.JsonUtils;

import io.searchbox.client.JestClient;

public class CanalClient {

    public static final String HOST_IP = "172.16.31.77";

    public static final int PORT = 11111;

    public static final String DEST = "example";

    public static final String USERNAME = "";

    public static final String PASSWORD = "";

    public static void synES(JestClient jestClient) {

        CanalConnector connector = CanalConnectors
                .newSingleConnector(new InetSocketAddress(HOST_IP, PORT), DEST, USERNAME, PASSWORD);

        int batchSize = 5 * 1024;

        while (true) {
            try {
                // 连接
                connector.connect();
                // 订阅
                connector.subscribe("scm_vendor.wt_calendar_config");

                Message message = connector.getWithoutAck(batchSize);
                long batchId = message.getId();
                if (batchId == -1 || message.getEntries().isEmpty()) {
                    // 空数据，可以进行sleep
                } else {
                    printEntries(jestClient, message.getEntries());
                }
                connector.ack(batchId);
            } finally {
                connector.disconnect();
            }
        }

    }

    public static void printEntries(JestClient jestClient, List<CanalEntry.Entry> entries) {

        for (CanalEntry.Entry entry : entries) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                    || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            CanalEntry.RowChange rowChage = null;
            try {
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            String sql = rowChage.getSql();

            System.out.println(sql);

            CanalEntry.Header header = entry.getHeader();
            String schemaName = header.getSchemaName();
            String tableName = header.getTableName();

            CanalEntry.EventType eventType = rowChage.getEventType();

            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == CanalEntry.EventType.INSERT) {
                    List<CanalEntry.Column> columns = rowData.getAfterColumnsList();
                    String json = JsonUtils.toJson(columnToMap(columns));
                    try {
                        String result = EsUtils.saveUpdateIndex(jestClient, schemaName, tableName, null, json);
                        System.out.println(result);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            }

            System.out.println(eventType);

        }
    }

    public static Map<String, String> columnToMap(List<CanalEntry.Column> columns) {

        Map<String, String> map = new LinkedHashMap<>();
        if (!CollectionUtils.isEmpty(columns)) {
            for (CanalEntry.Column column : columns) {
                map.put(column.getName(), column.getValue());
            }
        }
        return map;
    }

}
