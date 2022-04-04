package com.atguigu;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.protocol.HTTP;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import java.io.IOException;

/**
 * @author wangfei
 * @project_name:esone
 * @package_name:com.atguigu null.java
 * @class_name: DocCRUD
 * @create 2022-02-17 20:18
 * @desc
 */
public class DocCRUD {
    public static void main(String[] args) throws IOException {

        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
//       创建客户端对象
        RestHighLevelClient client = new RestHighLevelClient(builder);
/*

        System.out.println("===========================1创建文档===============================");
//        创建新建文档的请求对象
        IndexRequest request = new IndexRequest();
//        设置操作的索引及新建的文档的id
        request.index("user").id("1002");
//        封装文档对象的数据
        User user = new User("tom", 30, "男");
//        创建映射对象
        ObjectMapper objectMapper = new ObjectMapper();
//        将文档数据对象封装到映射对象中，返回json对象
        String userJoson = objectMapper.writeValueAsString(user);
//        将json封装的请求对象中
        request.source(userJoson, XContentType.JSON);
//        通过客户端发送请求，获取响应对象

        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        System.out.println("indexResponse.getIndex() = " + indexResponse.getIndex());
        System.out.println("indexResponse.getId() = " + indexResponse.getId());
        System.out.println("indexResponse.getResult() = " + indexResponse.getResult());

*/
/*

//        查询文档
        System.out.println("===========================2查询文档===============================");
//        1.创建查询文档的请求对象
        GetRequest getRequest = new GetRequest().index("user").id("1002");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println("1getResponse.getIndex() = " + getResponse.getIndex());
        System.out.println("2getResponse.getSourceAsString() = " + getResponse.getSourceAsString());
        System.out.println("3getResponse.getType() = " + getResponse.getType());
        System.out.println("4getResponse.getId() = " + getResponse.getId());
*/
/*

//        修改文档
        System.out.println("===========================3修改文档===============================");
//        1.创建查询文档的请求对象
        UpdateRequest updateRequest = new UpdateRequest();
//        传入修改i的数据
        updateRequest.index("user").id("1002");
        updateRequest.doc(XContentType.JSON,"sex","女");
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println("updateResponse.getId() = " + updateResponse.getId());
        System.out.println("updateResponse.getResult() = " + updateResponse.getResult());
        System.out.println("updateResponse.getIndex() = " + updateResponse.getIndex());
*/

/*
//        删除文档
        System.out.println("===========================4删除文档===============================");
//        1.创建删除文档的请求对象
        DeleteRequest deleteRequest = new DeleteRequest().index("user").id("1002");
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println("deleteRequest.toString() = " + deleteRequest.toString());
*/

        System.out.println("===========================5批量新增文档===============================");
//        1.创建批量新增文档的请求对象
        BulkRequest bulkRequest = new BulkRequest();

//        封装需要添加的index，doc相关数据
        IndexRequest source1 = new IndexRequest().index("user").id("1004").source(XContentType.JSON, "name", "jack","sex","男");
        IndexRequest source2 = new IndexRequest().index("user").id("1002").source(XContentType.JSON, "name", "rose","sex","女");
        IndexRequest source3 = new IndexRequest().index("user").id("1003").source(XContentType.JSON, "name", "jack","sex","男");

        bulkRequest.add(source1);
        bulkRequest.add(source2);
        bulkRequest.add(source3);
        //客户端发送请求
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println("bulkResponse.buildFailureMessage() = " + bulkResponse.buildFailureMessage());
        System.out.println("bulkResponse.getTook() = " + bulkResponse.getTook());
        System.out.println("bulkResponse.getItems() = " + bulkResponse.getItems());

      /*  System.out.println("===========================5批量删除文档===============================");
//        1.批量删除文档的请求对象引用上面的

        DeleteRequest user = new DeleteRequest("user").id("1003");
        DeleteRequest user2 = new DeleteRequest("user").id("1002");

        bulkRequest.add(user);
        bulkRequest.add(user2);

        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println("bulkResponse.getItems() = " + bulkResponse.getItems());
        System.out.println("bulkResponse.getTook() = " + bulkResponse.getTook());

        client.close();*/
    }


    @Test
    public void testQuery() throws IOException {

        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
        RestHighLevelClient client = new RestHighLevelClient(builder);
//        查询文档
        System.out.println("===========================查询文档===============================");
//        1.创建查询文档的请求对象
        GetRequest getRequest = new GetRequest().index("user").id("1001");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println("1getResponse.getIndex() = " + getResponse.getIndex());
        System.out.println("2getResponse.getSourceAsString() = " + getResponse.getSourceAsString());
        System.out.println("3getResponse.getType() = " + getResponse.getType());
        System.out.println("4getResponse.getId() = " + getResponse.getId());

        client.close();
    }
//============================高级查询=============================================
//============================高级查询=============================================
//============================高级查询=============================================

@Test
    public void getAll() throws IOException {
    RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
    RestHighLevelClient client = new RestHighLevelClient(builder);


    //  创建搜索请求对象
        SearchRequest request = new SearchRequest();
    //    设置要查询的索引
        request.indices("user");
    //    构建查询的请求体

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    //    查询所有数据
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        request.source(sourceBuilder);

    SearchResponse response = client.search(request, RequestOptions.DEFAULT);
    // 查询匹配
    SearchHits hits = response.getHits();
    System.out.println("response.getTook() = " + response.getTook());
    System.out.println("response.isTimedOut() = " + response.isTimedOut());
    System.out.println("response.getTotalShards() = " + response.getTotalShards());
    System.out.println("hits.getTotalHits() = " + hits.getTotalHits());
    System.out.println("hits.getMaxScore() = " + hits.getMaxScore());
    System.out.println( "hits  数据=========================");
    for (SearchHit hit : hits) {
        System.out.println(hit.getSourceAsString());
    }

}
//============================高级term查询=============================================
//============================高级term查询=============================================
//============================高级term查询=============================================
    @Test
public void termTest() throws IOException {
//    创建客户端
    RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
    RestHighLevelClient client = new RestHighLevelClient(builder);
//    创建搜索请求对象
    SearchRequest request = new SearchRequest().indices("user");

//    创建查询的请求体
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    sourceBuilder.query(QueryBuilders.termQuery("name","rose"));
    request.source(sourceBuilder);
//   发送请求对象，获取请求结果对象

    SearchResponse response = client.search(request, RequestOptions.DEFAULT);

//    取出查询doc数据
    SearchHits hits = response.getHits();
    System.out.println("response.getTook() = " + response.getTook());
    System.out.println("hits.getTotalHits() = " + hits.getTotalHits());
    System.out.println("hits.getMaxScore() = " + hits.getMaxScore());

    for (SearchHit hit : hits) {
        System.out.println("hit.getId() = " + hit.getId());
        System.out.println("hit.getSourceAsString() = " + hit.getSourceAsString());
    }
}
//============================分页查询=============================================
    @Test
    public void  limit() throws IOException {
//        1创建客户端
        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
        RestHighLevelClient client = new RestHighLevelClient(builder);
//        2创建请求对象
        SearchRequest request = new SearchRequest().indices("user");
//        3.创建请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());

//        4.设置分页条件
        sourceBuilder.from(0);
        sourceBuilder.size(2);

//        5.将请求体封装到请求对象中
        request.source(sourceBuilder);

//        6.通过客户端发送请求，获取查询响应对象
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

//        7.获取查询对象中的数据并打印
        SearchHits hits = response.getHits();
        System.out.println("======================");
        System.out.println("response.getTook() = " + response.getTook());
        System.out.println("response.isTimedOut() = " + response.isTimedOut());
        System.out.println("hits.getMaxScore() = " + hits.getMaxScore());
        System.out.println("hits.getTotalHits() = " + hits.getTotalHits());

        for (SearchHit hit : hits) {
            System.out.println("hit.getSourceAsString() = " + hit.getSourceAsString());
        }

    }
//============================排序查询=============================================
@Test
    public void sortTest() throws IOException {
//        1.创建客户端
        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
        RestHighLevelClient client = new RestHighLevelClient(builder);
//        2.创建请求对象
        SearchRequest request = new SearchRequest().indices("user");
//        3.创建请求体

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());

//        4.设置排序
        sourceBuilder.sort("age", SortOrder.ASC);

//        5.将请求体封装到请求对象中
        request.source(sourceBuilder);
//        6.通过客户端发送请求，获取响应对象
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();
        System.out.println("response.getTook() = " + response.getTook());
        System.out.println("response.isTimedOut() = " + response.isTimedOut());

        System.out.println("hits.getTotalHits() = " + hits.getTotalHits());
        System.out.println("hits.getMaxScore() = " + hits.getMaxScore());


        for (SearchHit hit : hits) {
            System.out.println("---------------------------");
            System.out.println("hit.getIndex() = " + hit.getIndex());
            System.out.println("hit.getSourceAsString() = " + hit.getSourceAsString());
        }


        client.close();
    }
//============================过滤字段查询=============================================
    @Test
    public void filterTest() throws IOException {
//        1.创建客户端
        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
        RestHighLevelClient client = new RestHighLevelClient(builder);
//        2.创建请求对象
        SearchRequest request = new SearchRequest().indices("user");
//        3.创建请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
//        4.设置过滤的字段
        String excludes[] = {};
        String includes[] = {"age"};
        sourceBuilder.fetchSource(includes,excludes);

//        5.将请求体封装到请求对象
        request.source(sourceBuilder);
//        6.通过客户端发送请求对象，获取响应对象
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

//        7.取出响应对象中的数据
        SearchHits hits = response.getHits();
        System.out.println("response.isTimedOut() = " + response.isTimedOut());
        System.out.println("response.getTook() = " + response.getTook());

        System.out.println("hits.getMaxScore() = " + hits.getMaxScore());
        System.out.println("hits.getTotalHits() = " + hits.getTotalHits());

        for (SearchHit hit : hits) {
            System.out.println("hit.getSourceAsString() = " + hit.getSourceAsString());
        }

        client.close();
    }
//============================bool查询=============================================
    @Test
    public void getBool() throws IOException {
        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
        RestHighLevelClient client = new RestHighLevelClient(builder);
        //创建请求对象
        SearchRequest request = new SearchRequest().indices("user");
        //创建请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //设置查询条件对象
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        //boolQueryBuilder.must(QueryBuilders.matchQuery("name","rose"));
       // boolQueryBuilder.mustNot(QueryBuilders.matchQuery("name","jack"));
       // boolQueryBuilder.should(QueryBuilders.matchQuery("sex","男"));
        //将查询条件封装的请求体
        sourceBuilder.query(boolQueryBuilder);
        //将请求体封装到请求对象中
        request.source(sourceBuilder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println("hit.getSourceAsString() = " + hit.getSourceAsString());

        }

    }
//============================范围查询=============================================
//============================模糊查询=============================================
//============================高亮查询=============================================
//============================聚合查询=============================================;
//============================分组查询=============================================

}
