package com.atguigu.gmall0105.publisher.service.impl;

import com.atguigu.gmall0105.publisher.service.EsService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class EsServiceImpl implements EsService {

    @Autowired
    JestClient jestClient;

    @Override
    public Long getDauTotal(String date) {
        String indexName = "gmall0105_dau_info_"+date+"-query";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder());
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).addType("_doc").build();
        try {
            SearchResult searchResult = jestClient.execute(search);
            return   searchResult.getTotal();

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("es 查询异常");
        }
    }

    @Override
    public Map getDauHour(String date) {
        String indexName = "gmall0105_dau_info_"+date+"-query";
        //构造查询语句
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsBuilder aggBuilder = AggregationBuilders.terms("groupby_hr").field("hr").size(24);
        searchSourceBuilder.aggregation(aggBuilder);

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).addType("_doc").build();
        try {
            SearchResult searchResult = jestClient.execute(search);
       //封装返回结果
            Map<String,Long> aggMap=new HashMap<>();
            if(searchResult.getAggregations().getTermsAggregation("groupby_hr")!=null){
                List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_hr").getBuckets();
                for (TermsAggregation.Entry bucket : buckets) {
                    aggMap.put( bucket.getKey(),bucket.getCount());
                }
            }
            return aggMap;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("es 查询异常");
        }

    }
}
