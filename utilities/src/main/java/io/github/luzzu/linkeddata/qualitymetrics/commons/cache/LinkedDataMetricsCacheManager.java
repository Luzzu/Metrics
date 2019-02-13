/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.commons.cache;

import io.github.luzzu.operations.cache.CacheManager;
import io.github.luzzu.operations.cache.CacheObject;

/**
 * @author Jeremy Debattista
 * 
 * This class communicates with Luzzu's Cache Manager,
 * storing resources which might be used in the future.
 * 
 */
public class LinkedDataMetricsCacheManager {

	public static final String HTTP_RESOURCE_CACHE = "http_resource_cache";
	public static final String VOCABULARY_CACHE = "vocabulary_cache";
	
	private static LinkedDataMetricsCacheManager instance = null;
	private CacheManager luzzuCM = CacheManager.getInstance();
	
	protected LinkedDataMetricsCacheManager(){
		luzzuCM.createNewCache(HTTP_RESOURCE_CACHE, 5000);
		luzzuCM.createNewCache(VOCABULARY_CACHE, 5000);
	};
	
	public static LinkedDataMetricsCacheManager getInstance(){
		if (instance == null) {
			instance = new LinkedDataMetricsCacheManager();
		}
		return instance;
	}
	
	public void addToCache(String cacheName, String key, CacheObject value){
		luzzuCM.addToCache(cacheName, key, value);
	}
	
	public boolean existsInCache(String cacheName, String key){
		return luzzuCM.existsInCache(cacheName, key);
	}
	
	public Object getFromCache(String cacheName, String key){
		return luzzuCM.getFromCache(cacheName, key);
	}
}
