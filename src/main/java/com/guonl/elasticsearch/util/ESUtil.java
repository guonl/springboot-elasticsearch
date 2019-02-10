package com.guonl.elasticsearch.util;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.List;

/**
 * Created by guonl
 * Date 2019/1/29 1:57 PM
 * Description: ES 工具
 */
public class ESUtil {
	private final Logger logger = LoggerFactory.getLogger(ESUtil.class);

	private TransportClient transportClient;

	public void setTransportClient(TransportClient transportClient) {
		this.transportClient = transportClient;
	}

	/**
	 * 新增或修改
	 *
	 * @param indexType
	 *            es类型
	 * @param bean
	 * @param clazz
	 * @param propName
	 *            id名
	 * @param <T>
	 * @return
	 * @throws Exception
	 */
	public <T> IndexResponse insertOrUpdate(String indexName, String indexType, T bean, Class<T> clazz, String propName)
			throws Exception {
		if (StringUtils.isBlank(indexName) || StringUtils.isBlank(indexType) || bean == null
				|| StringUtils.isBlank(propName)) {
			return null;
		}
		Field field = clazz.getDeclaredField(propName);
		field.setAccessible(true);
		Object object = field.get(bean);
		if (object != null) {
			SearchRequestBuilder query = transportClient.prepareSearch(indexName).setTypes(indexType)
					.setQuery(QueryBuilders.termQuery(propName, object.toString()));
			SearchResponse searchResponse = query.get();
			long totalHits = searchResponse.getHits().getTotalHits();
			if (totalHits == 0) {
				insert(indexName, indexType, bean, clazz, propName);
			} else {
				for (SearchHit searchHitFields : searchResponse.getHits()) {
					update(indexName, indexType, bean, clazz, propName);
				}
			}
		}
		return null;

	}

	/**
	 * 新增
	 *
	 * @param indexType
	 *            es类型
	 * @param bean
	 * @param clazz
	 * @param propName
	 *            id名
	 * @param <T>
	 * @return
	 * @throws Exception
	 */
	public <T> IndexResponse insert(String indexName, String indexType, T bean, Class<T> clazz, String propName)
			throws Exception {
		if (StringUtils.isBlank(indexName) || StringUtils.isBlank(indexType) || bean == null
				|| StringUtils.isBlank(propName)) {
			return null;
		}
		Field field = clazz.getDeclaredField(propName);
		field.setAccessible(true);
		Object object = field.get(bean);
		if (object != null) {
			IndexResponse response = this.transportClient.prepareIndex(indexName, indexType)
					.setId(String.valueOf(object.toString())).setSource(JSON.toJSONString(bean)).get();
			return response;
		}
		return null;

	}

	/**
	 * 批量新增
	 *
	 * @param indexType
	 *            es类型
	 * @param beanList
	 *            列表
	 * @param clazz
	 *            类
	 * @param propName
	 * @param <T>
	 * @throws Exception
	 */
	public <T> void batchInsert(String indexName, String indexType, List<T> beanList, Class<T> clazz, String propName)
			throws Exception {
		if (StringUtils.isBlank(indexName) || StringUtils.isBlank(indexType) || beanList == null || beanList.isEmpty()
				|| StringUtils.isBlank(propName)) {
			return;
		}
		IndicesExistsRequest indicesExistsRequest = new IndicesExistsRequest(indexName);
		IndicesExistsResponse indicesExistsResponse = transportClient.admin().indices().exists(indicesExistsRequest)
				.actionGet();
		if (indicesExistsResponse.isExists()) {
			// DeleteIndexResponse deleteIndexResponse =
			// transportClient.admin().indices().prepareDelete(indexName).execute().actionGet();
			// logger.info("delete es index: {}, result: {}",
			// EsKey.USED_PROMOTION_INDEX_NAME,
			// deleteIndexResponse.isAcknowledged());
		} else {
			CreateIndexRequestBuilder cib = transportClient.admin().indices().prepareCreate(indexName);
			CreateIndexResponse res = cib.execute().actionGet();
			logger.info(JSON.toJSONString(res));
		}

		BulkRequestBuilder bulkRequest = transportClient.prepareBulk();
		for (int i = 0; i < beanList.size(); i++) {
			T bean = beanList.get(i);

			Field field = clazz.getDeclaredField(propName);
			field.setAccessible(true);
			Object object = field.get(bean);
			if (object != null) {
				bulkRequest.add(transportClient.prepareIndex(indexName, indexType).setId(object.toString())
						.setSource(JSON.toJSONString(bean)));

				if (i > 0 && i % 10000 == 0) {
					BulkResponse bulkItemResponses = bulkRequest.get();
					if (bulkItemResponses.hasFailures()) {
						logger.error(bulkItemResponses.buildFailureMessage());
					}
					bulkRequest = transportClient.prepareBulk();
					logger.info("提交了：" + i);
				}
			}

		}
		bulkRequest.get();
	}

	/**
	 * 修改
	 *
	 * @param indexType
	 *            es类型
	 * @param bean
	 * @param clazz
	 * @param propName
	 *            id名
	 * @param <T>
	 * @return
	 * @throws Exception
	 */
	public <T> UpdateResponse update(String indexName, String indexType, T bean, Class<T> clazz, String propName)
			throws Exception {
		if (StringUtils.isBlank(indexName) || StringUtils.isBlank(indexType) || bean == null
				|| StringUtils.isBlank(propName)) {
			return null;
		}
		Field field = clazz.getDeclaredField(propName);
		field.setAccessible(true);
		Object object = field.get(bean);
		if (object != null) {
			UpdateResponse response = this.transportClient.prepareUpdate(indexName, indexType, object.toString())
					.setDoc(JSON.toJSONString(bean)).get();
			return response;
		}

		return null;

	}

	/**
	 * 删除
	 *
	 * @param indexType
	 *            es类型
	 * @param id
	 * @return
	 */
	public DeleteResponse delete(String indexName, String indexType, String id) {
		if (StringUtils.isBlank(indexName) || StringUtils.isBlank(indexType) || StringUtils.isBlank(id)) {
			return null;
		}
		DeleteResponse deleteResponse = transportClient.prepareDelete(indexName, indexType, id).execute().actionGet();
		return deleteResponse;
	}

}
