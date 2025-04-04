package com.amadeus.dataio.testutils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 * Provides the Hadoop LocalFileSystem for tests needing direct access to the FileSystem.
 *
 * Provides a dedicated instance initialized before All tests and automatically closed after all tests.
 * Automatically delete dataio framework tmp directory and sub-directories, before closing the filesystem.
 *
 * e.g.
 * {{{
 *   class MyClassTest extends AnyWordSpec with FileSystemIntegration{
 *      // provided by FileSystemIntegration:
 *      // fs: FileSystem
 *      // val tmpPath: String
 *   }
 * }}}
 */
trait FileSystemSuite extends Suite with BeforeAndAfterAll {
  val tmpPath = "file:///tmp/dataiofwk/"

  var fs: FileSystem = _

  override protected def beforeAll(): Unit = {
    fs = FileSystem.newInstance(new Configuration())
  }

  override protected def afterAll(): Unit = {
    fs.delete(new Path(tmpPath), true)
    fs.close()
  }

}
