package com.amadeus.dataio.test

import org.apache.spark.sql.DataFrame

/** A singleton object that provides in-memory storage for datasets during testing.
  *
  * The `TestDataStore` acts as a temporary, in-memory data store used to simulate reading and writing datasets
  * during tests. It provides methods for saving, loading, and removing datasets by a unique path or name.
  * This is useful for unit testing scenarios where actual external storage (e.g., databases, file systems)
  * is not necessary or desirable. It is particularly useful when testing the behavior of code that interacts
  * with datasets, but where persistence or network access is not required.
  *
  * Datasets are stored in a mutable `Map` where the keys are paths (used as unique identifiers) and the values
  * are the corresponding `DataFrame` objects.
  *
  * This object supports namespacing through the use of paths, allowing tests to isolate their data to prevent
  * collisions between datasets of different tests or test suites.
  */
object TestDataStore {
  private val datasets = scala.collection.mutable.Map[String, DataFrame]()

  /** Adds a DataFrame to the collection with the given path.
    *
    * @param path The path (or namespace) associated with the DataFrame.
    * @param df   The DataFrame to save.
    * @note If a DataFrame with the given path already exists, it will be overwritten.
    *       This means that the previous dataset will be replaced by the new one.
    */
  def save(path: String, df: DataFrame): Unit = {
    datasets(path) = df
  }

  /** Retrieves a DataFrame from the collection by its path.
    *
    * @param path The path of the DataFrame to retrieve.
    * @return The `DataFrame` associated with the given path.
    * @throws Exception if the DataFrame with the given path is not found.
    * @note This method throws an exception if no dataset is found with the specified path. Ensure that the
    *       correct path is provided when accessing the datasets.
    */
  def load(path: String): DataFrame = {
    datasets.getOrElse(path, throw new Exception(s"Dataset `$path` not found. Check your test configuration."))
  }

  /** Removes a DataFrame from the collection by its path.
    *
    * @param path The path of the DataFrame to remove.
    * @note If no DataFrame with the specified path exists, nothing happens. This ensures that calling
    *       `delete` on a non-existing dataset will not result in an error.
    */
  def delete(path: String): Unit = {
    datasets.remove(path)
  }

  /** Clears all datasets from the in-memory data store.
    *
    * This method removes all entries from the internal `datasets` map, effectively
    * resetting the data store. After calling this method, no datasets will remain in memory.
    * This can be useful for cleaning up between tests or resetting the state of the store.
    * It can help avoid unintended side effects between test cases that may share the same test environment.
    */
  def clear(): Unit = {
    datasets.clear()
  }

  /** Clears datasets from the in-memory data store based on a path prefix.
    *
    * This method removes all datasets whose path starts with the specified prefix.
    * It allows for more granular clearing of datasets, especially in cases where you want to
    * clear a specific namespace or subset of datasets while keeping others intact.
    *
    * For example, if you call `clear("memory://my/path/")`, it will remove all datasets
    * starting with that prefix, leaving others (e.g., "memory://my/other/path/") unaffected.
    *
    * @param prefix The prefix used to match dataset paths. Any dataset whose path starts with this
    *               prefix will be removed from the store.
    */
  def clear(prefix: String): Unit = {
    datasets.keys.filter(_.startsWith(prefix)).foreach(datasets.remove)
  }
}
