package com.atguigu.gmall0105.realtime.util

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index, Search, SearchResult}
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, TermQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.highlight.{HighlightBuilder, HighlightField}
import org.elasticsearch.search.sort.SortOrder

object MyEsUtil {

  var factory:JestClientFactory=null;

  def getClient:JestClient ={
    if(factory==null)build();
    factory.getObject

  }

  def  build(): Unit ={
    factory=new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hdp1:9200" )
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000).readTimeout(10000).build())

  }


  def addDoc(): Unit ={
    val jest: JestClient = getClient
    //build 设计模式
    // 可转化为json对象    HashMap
    val index = new Index.Builder(Movie0105("0104","龙岭迷窟","鬼吹灯")).index("movie0105_test_20200619").`type`("_doc").id("0104").build()
    val message: String = jest.execute(index).getErrorMessage
    if(message!=null){
      println(message)
    }
    jest.close()
  }
  def  bulkDoc( sourceList:List[(String,Any)],indexName:String): Unit ={
    if(sourceList!=null&&sourceList.size>0){
      val jest: JestClient = getClient
      val bulkBuilder = new Bulk.Builder          //构造批次操作
      for ((id,source) <- sourceList ) {
        val index = new Index.Builder(source).index(indexName).`type`("_doc").id(id).build()
        bulkBuilder.addAction(index)
      }
      val bulk: Bulk = bulkBuilder.build()
      val result: BulkResult = jest.execute(bulk)
      val items: util.List[BulkResult#BulkResultItem] = result.getItems
      println("保存到ES:"+items.size()+"条数")
      jest.close()
    }
  }


  // 把结构封装的Map 必须使用java 的   ，不能使用scala
  def queryDoc(): Unit ={
    val jest: JestClient = getClient
    val query="{\n  \"query\": {\n    \"bool\": {\n      \"must\": [\n        { \"match\": {\n          \"name\": \"operation\"\n        }}\n      ],\n      \"filter\": {\n            \"term\": {\n           \"actorList.name.keyword\": \"zhang han yu\"\n         }\n      }\n    }\n  },\n  \"from\": 0\n  , \"size\": 20\n  ,\"sort\": [\n    {\n      \"doubanScore\": {\n        \"order\": \"desc\"\n      }\n    }\n  ]\n \n}"

    val searchSourceBuilder = new SearchSourceBuilder()

    val boolQueryBuilder = new BoolQueryBuilder
    boolQueryBuilder.must(new MatchQueryBuilder("name","red"))
    boolQueryBuilder.filter(new TermQueryBuilder("actorList.name.keyword","zhang han yu"))
    searchSourceBuilder.query(boolQueryBuilder)
    searchSourceBuilder.from(0).size(20)
    searchSourceBuilder.sort("doubanScore",SortOrder.DESC)
    searchSourceBuilder.highlight(new HighlightBuilder().field("name"))
    val query2: String = searchSourceBuilder.toString

    println(query2)
    val search= new Search.Builder(query2).addIndex("movie_index0105").addType("movie").build()
    val result: SearchResult = jest.execute(search)
    val hits: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits( classOf[util.Map[String,Any]])
    import scala.collection.JavaConversions._
    for (hit <- hits ) {
      println(hit.source.mkString(","))
    }

    jest.close()
  }

  def main(args: Array[String]): Unit = {
    queryDoc()
  }


  case class Movie0105 (id:String ,movie_name:String,name:String);


}
