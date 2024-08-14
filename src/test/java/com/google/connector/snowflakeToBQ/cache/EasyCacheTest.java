package com.google.connector.snowflakeToBQ.cache;

import static org.junit.jupiter.api.Assertions.*;

import com.google.connector.snowflakeToBQ.base.AbstractTestBase;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Test class for {@link EasyCache}. This class extends {@link AbstractTestBase}
 * and contains various test cases to ensure the functionality of the {@link EasyCache} class.
 * The tests cover basic operations like putting and getting values, TTL expiration,
 * max capacity removal, multithreaded access, and cache clearing.
 */
public class EasyCacheTest extends AbstractTestBase {

  @Autowired
  private EasyCache<String, String> cache;

  /**
   * Tests the basic functionality of putting and getting values from the cache.
   * Ensures that a value stored in the cache can be retrieved correctly.
   */
  @Test
  public void testPutAndGet() {
    cache.put("key1", "value1");
    assertEquals("value1", cache.get("key1"));
  }

  /**
   * Tests the Time-To-Live (TTL) expiration functionality of the cache.
   * Puts a value into the cache, waits for the TTL to expire, and then
   * checks that the value is no longer in the cache.
   *
   * @throws InterruptedException if the thread is interrupted while sleeping
   */
  @Test
  public void testTtlExpiration() throws InterruptedException {
    cache.put("key1", "value1");
    Thread.sleep(1100); // Wait for TTL to expire
    assertNull(cache.get("key1"));
  }

  /**
   * Tests the maximum capacity removal functionality of the cache.
   * Puts multiple values into the cache and verifies that the oldest entry
   * is removed when the cache exceeds its maximum capacity.
   */
  @Test
  public void testMaxCapacityRemoval() {
    cache.put("key1", "value1");
    cache.put("key2", "value2");
    cache.put("key3", "value3");
    cache.put("key4", "value4"); // This should remove "key1"

    assertNull(cache.get("key1"));
    assertEquals("value2", cache.get("key2"));
    assertEquals("value3", cache.get("key3"));
    assertEquals("value4", cache.get("key4"));
  }

  /**
   * Tests the multithreaded access to the cache.
   * Simulates multiple threads putting values into the cache concurrently
   * and verifies that all values are stored and retrievable.
   *
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  @Test
  public void testMultithreadedAccess() throws InterruptedException {
    int numThreads = 3;
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);
long start=System.currentTimeMillis();
    for (int i = 0; i < numThreads; i++) {
      final int index = i;
      executorService.submit(
              () -> {
                cache.put("key" + index, "value" + index);
                latch.countDown();
              });
    }

    latch.await(); // Wait for all threads to finish
    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.MINUTES);
    System.out.println("total to,e"+(System.currentTimeMillis()-start));
    System.out.println("the cache size:"+cache);
    for (int i = 0; i < numThreads; i++) {
      assertEquals("value" + i, cache.get("key" + i));
    }
  }

  /**
   * Tests the cache clearing functionality.
   * Puts a value into the cache, clears the cache, and then verifies that
   * the value is no longer present in the cache.
   */
  @Test
  public void testClearCache() {
    cache.put("key1", "value1");
    cache.clear();
    assertNull(cache.get("key1"));
  }

  /**
   * Tests simultaneous put and get operations from the cache using multiple threads.
   * Ensures that put and get operations do not interfere with each other and
   * that the value is correctly put and retrieved.
   *
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  @Test
  public void testSimultaneousPutAndGet() throws InterruptedException {
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    CountDownLatch latch = new CountDownLatch(2);

    executorService.submit(
            () -> {
              cache.put("key1", "value1");
              latch.countDown();
            });

    executorService.submit(
            () -> {
              assertEquals("value1", cache.get("key1"));
              latch.countDown();
            });

    latch.await();
    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.MINUTES);
  }
}
