package com.atguigu;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;


/**
 * @author wangfei
 * @create 2022-04-02
 * @desc
 */
public class creatIndex {
    public static void main(String[] args) throws Exception{
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http"))
        );

        // 创建索引 - 请求对象
        CreateIndexRequest request = new CreateIndexRequest("user");
// 发送请求，获取响应
        CreateIndexResponse response = client.indices().create(request,
                RequestOptions.DEFAULT);
        boolean acknowledged = response.isAcknowledged();
// 响应状态
        System.out.println("操作状态 = " + acknowledged);
// 关闭客户端连接
        client.close();
    }
}
