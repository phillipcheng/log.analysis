package etl.util;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.cache.Cache;
import com.google.common.cache.LoadingCache;

public class CacheMap<T> implements Map<String, T> {
	public static final Logger logger = LogManager.getLogger(CacheMap.class);
	private Cache<String, T> cache;
	
	public CacheMap(Cache<String, T> cache) {
		this.cache = cache;
	}

	public int size() {
		return (int) cache.size();
	}

	public boolean isEmpty() {
		return cache.size() == 0;
	}

	public boolean containsKey(Object key) {
		return cache.asMap().containsKey(key);
	}

	public boolean containsValue(Object value) {
		return cache.asMap().containsValue(value);
	}
	
	public T get(Object key) {
		if (cache instanceof LoadingCache)
			try {
				return ((LoadingCache<String, T>)cache).get((String) key);
			} catch (ExecutionException e) {
				logger.error(e.getMessage(), e);
				return null;
			}
		else
			return cache.getIfPresent((String) key);

	}

	public T put(String key, T value) {
		T previous = cache.getIfPresent((String) key);
		cache.put(key, value);
		return previous;
	}

	public T remove(Object key) {
		T previous = cache.getIfPresent((String) key);
		cache.invalidate(key);
		return previous;
	}

	public void putAll(Map<? extends String, ? extends T> m) {
		if (m != null)
			for (Map.Entry<? extends String, ? extends T> e: m.entrySet()) {
				cache.put(e.getKey(), e.getValue());
			}
	}

	public void clear() {
		cache.invalidateAll();
	}

	public Set<String> keySet() {
		return cache.asMap().keySet();
	}

	public Collection<T> values() {
		return cache.asMap().values();
	}

	public Set<java.util.Map.Entry<String, T>> entrySet() {
		return cache.asMap().entrySet();
	}

}
