package com.amadeus.dataio.test

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfter, TestSuite}

/**
 * Provides the Hadoop LocalFileSystem for tests needing direct access to an instance of FileSystem.
 *
 * Provides a dedicated instance initialized before each test and automatically closed after each test, providing as
 * much isolation as possible between tests. It also deletes the dataio-test temporary directory (/tmp/dataio-test/) and
 * sub-directories, before closing the FileSystem.
 *
 * e.g.
 * {{{
 *   class MyClassTest extends WordSpec with FileSystemSpec{
 *      // provided by FileSystemSpec:
 *      // fs: FileSystem
 *      // val tmpPath: String = "file:///tmp/dataio-test/"
 *   }
 * }}}
 */
trait FileSystemSpec extends TestSuite with BeforeAndAfter {
  val tmpPath = "file:///tmp/dataio-test/"

  var fs: FileSystem = _

  before {
    fs = FileSystem.newInstance(new Configuration())
  }

  after {
    fs.delete(new Path(tmpPath), true)
    fs.close()
  }
}
