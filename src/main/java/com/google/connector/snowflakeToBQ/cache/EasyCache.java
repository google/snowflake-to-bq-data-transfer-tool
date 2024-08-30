/*
 * Copyright 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.connector.snowflakeToBQ.cache;

import java.util.LinkedHashMap;
import java.util.Map;

import com.google.connector.snowflakeToBQ.repository.ClosableResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * A thread-safe cache implementation with time-to-live (TTL) support and a maximum size.
 *
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of mapped values
 */
@Component
public class EasyCache<K, V> {

  private final long ttlMillis;
  private final Map<K, CacheObject<V>> cache;
  private static final Logger log = LoggerFactory.getLogger(EasyCache.class);

  /**
   * Constructs an EasyCache with the specified maximum size and TTL.
   *
   * @param maxSize the maximum number of entries the cache can hold
   * @param ttlMillis the time-to-live for cache entries in milliseconds
   */
  public EasyCache(
      @Value("${cache.maxSize}") int maxSize, @Value("${cache.ttlMillis}") long ttlMillis) {
    log.info("Max Size:{},ttlMillis:{}", maxSize, ttlMillis);
    this.ttlMillis = ttlMillis;
    this.cache =
        new LinkedHashMap<>(maxSize, 0.75f, true) {
          @Override
          protected boolean removeEldestEntry(Map.Entry<K, CacheObject<V>> eldest) {
            // Remove the eldest entry if size exceeds maxSize
            log.info("Current cache size:{}", cache.size());
            if (size() > maxSize) {
              // Closing the resources if required
              closeIfNecessary(eldest.getValue().value);
              return true;
            }
            // return false if size is within the limit of maxsize
            return false;
          }
        };
  }

  /**
   * Adds a new entry to the cache with the specified key and value.
   *
   * @param key the key with which the specified value is to be associated
   * @param value the value to be associated with the specified key
   */
  public synchronized void put(K key, V value) {
    cache.put(key, new CacheObject<>(value, System.currentTimeMillis()));
  }

  /**
   * Retrieves the value associated with the specified key.
   *
   * @param key the key whose associated value is to be returned
   * @return the value associated with the specified key, or {@code null} if the key does not exist
   *     in the cache or the entry has expired
   */
  public synchronized V get(K key) {
    CacheObject<V> cacheObject = cache.get(key);
    if (cacheObject != null && (System.currentTimeMillis() - cacheObject.timestamp) < ttlMillis) {
      return cacheObject.value;
    } else {
      // Remove expired entry
      log.info(
          "Key:{}, has pass the defined TTL time:{} in application.properties, hence has been removed and new entry has been added.\n Cache Size:{}",
          key,
          this.ttlMillis,
          cache.size());
      CacheObject<V> oldCacheObject = cache.get(key);
      if (oldCacheObject != null) {
        // Closing the resources if required
        closeIfNecessary(oldCacheObject.value);
      }
      cache.remove(key);
      return null;
    }
  }

  /** Clears all entries from the cache. */
  public synchronized void clear() {
    cache.values().forEach(cacheObject -> closeIfNecessary(cacheObject.value));
    cache.clear();
  }

  /**
   * This method closes the resources(values) in the cache, if they are closable(implementing
   * the @{@link ClosableResource}) interface. If the object saved is not closable then this method
   * will just pass.
   *
   * @param value The value of the map, which is the cacheed object.
   */
  private void closeIfNecessary(V value) {
    if (value instanceof ClosableResource) {
      ((ClosableResource) value).closeResource();
    }
  }

  /**
   * Represents a cache entry containing a value and the timestamp when it was added to the cache.
   *
   * @param <V> the type of the cached value
   */
  private static class CacheObject<V> {
    final V value;
    final long timestamp;

    /**
     * Constructs a CacheObject with the specified value and timestamp.
     *
     * @param value the cached value
     * @param timestamp the time when the value was added to the cache
     */
    CacheObject(V value, long timestamp) {
      this.value = value;
      this.timestamp = timestamp;
    }
  }
}
