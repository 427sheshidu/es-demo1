package com.atguigu;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;

import java.io.IOException;

/**
 * @author wangfei
 * @project_name:esone
 * @package_name:com.atguigu null.java
 * @class_name: EsTest
 * @create 2022-02-17 18:46
 * @desc:-----索引操作--------
 */
public class EsTest {

    public static void main(String[] args) throws IOException {

        //builder：构建器，生成器
        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
        //创建客户端对象
        RestHighLevelClient client = new RestHighLevelClient(builder);
////一、创建索引---------------------------------------------------------------------------------------
////       创建添加索引请求对象
//        CreateIndexRequest request = new CreateIndexRequest("user");
////        通过客户端对象，发送请求，获取响应
//        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
//        boolean flag = response.isAcknowledged();
//        System.out.println("操作状态 = " + flag);
//二、获取索引
//      创建获取索引的对象
        GetIndexRequest getIndexRequest = new GetIndexRequest("user");
//        通过客户端请求，发送请求,获取响应的索引数据
        GetIndexResponse response1 = client.indices().get(getIndexRequest, RequestOptions.DEFAULT);

        System.out.println("aliases:"+response1.getAliases());
        System.out.println("mappings:"+response1.getMappings());
        System.out.println("settings:"+response1.getSettings());
//三、删除索引
//        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("user");
//        AcknowledgedResponse response3 = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
//        boolean flag = response3.isAcknowledged();
//        System.out.println("操作结构 = " + flag);


        client.close();
    }
}
