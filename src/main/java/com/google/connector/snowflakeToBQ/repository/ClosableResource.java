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

package com.google.connector.snowflakeToBQ.repository;

/**
 * An interface that defines a resource with a closeable operation.
 *
 * <p>Implementing classes should provide the logic to release or clean up
 * any resources that need to be managed explicitly, such as database connections,
 * network connections, file handles, etc. This interface allows such classes
 * to integrate with resource management in the {@link com.google.connector.snowflakeToBQ.cache.EasyCache} class.</p>
 *
 * <p>This interface is particularly useful for resources that are stored in a cache
 * and need to be cleaned up when they are removed from the cache or when the cache is cleared.</p>
 *
 * <p>Example usage:</p>
 * <pre>
 * public class MyResource implements ClosableResource {
 *     // Resource-specific fields and methods
 *
 *     {@literal @}Override
 *     public void closeResource() {
 *         // Logic to clean up or release resources
 *     }
 * }
 * </pre>
 */
public interface ClosableResource {
    /**
     * Closes the resource, releasing any underlying resources such as connections,
     * streams, or handles. Implementing classes should ensure that this method
     * can be called multiple times safely.
     */
    void closeResource();
}
