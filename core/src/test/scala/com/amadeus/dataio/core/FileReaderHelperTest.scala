package com.amadeus.dataio.core

import com.amadeus.dataio.testutils.FileSystemSuite
import org.apache.hadoop.fs.FileSystem
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.FileNotFoundException

class FileReaderHelperTest extends AnyWordSpec with Matchers with FileSystemSuite {
  lazy val inputPath: String = getClass.getResource("/core/FileReaderHelper/input.csv").getPath

  val inputContent: String = """date;from;to
                       |2021-08-19;Paris;Vienna
                       |2022-03-02;Turin:Nice""".stripMargin

  "toByteArray" when {
    "given a proper path and filesystem" should {
      "return the content of the file as a Array[Byte]" in {
        val bytes = FileReaderHelper.toByteArray(inputPath, fs)
        bytes should contain theSameElementsAs inputContent.getBytes
        bytes.isInstanceOf[Array[Byte]] shouldBe true
      }
    }

    "given the path of a file which does not exist" should {
      "throw a FileNotFoundException" in assertFileNotFound(FileReaderHelper.toByteArray)
    }

    "given any null or empty argument" should {
      "throw an IllegalArgumentException" in assertIllegalArguments(FileReaderHelper.toByteArray)
    }
  }

  "toString" when {
    "given a proper path and filesystem" should {
      "return the content of the file as a String" in {
        val str = FileReaderHelper.toString(inputPath, fs)
        str shouldBe inputContent
        str.isInstanceOf[String] shouldBe true
      }
    }

    "given the path of a file which does not exist" should {
      "throw a FileNotFoundException" in {
        a[FileNotFoundException] shouldBe thrownBy(FileReaderHelper.toString("/core/FileReaderHelper/not_found.csv", fs))
      }
    }

    "given any null or empty argument" should {
      "throw an IllegalArgumentException" in {
        an[IllegalArgumentException] shouldBe thrownBy(FileReaderHelper.toString(inputPath, null))
        an[IllegalArgumentException] shouldBe thrownBy(FileReaderHelper.toString(null, fs))
        an[IllegalArgumentException] shouldBe thrownBy(FileReaderHelper.toString("", fs))
      }
    }
  }

  "toZipByteArray" when {
    "given a proper path and filesystem" should {
      "return an Array[Byte] smaller than toByteArray" in {
        val inputToZipPath = getClass.getResource("/core/FileReaderHelper/input_to_zip.csv").getPath
        val bytes          = FileReaderHelper.toByteArray(inputToZipPath, fs)
        val zippedBytes    = FileReaderHelper.toZipByteArray(inputToZipPath, fs)

        zippedBytes.isInstanceOf[Array[Byte]] shouldBe true
        zippedBytes.length < bytes.length shouldBe true
      }
    }

    "given the path of a file which does not exist" should {
      "throw a FileNotFoundException" in assertFileNotFound(FileReaderHelper.toZipByteArray)
    }

    "given any null or empty argument" should {
      "throw an IllegalArgumentException" in assertIllegalArguments(FileReaderHelper.toZipByteArray)
    }
  }

  /**
   * Asserts the throw of a FileNotFoundException when the given function receives
   * the path of a file which does not exist as argument.
   * @param func A function expecting a path String as argument.
   */
  def assertFileNotFound(func: (String, FileSystem) => AnyRef): Unit = {
    a[FileNotFoundException] shouldBe thrownBy(func("/core/FileReaderHelper/notFound.csv", fs))
  }

  /**
   * Asserts the throw of an IllegalArgument exception when a function receives
   * a wrong (null or empty) path and/or FileSystem.
   * This is assuming that the given function expects a path String and a FileSystem
   * as arguments.
   * @param func The function to assert. It expects a path String and a FileSystem as arguments.
   */
  def assertIllegalArguments(func: (String, FileSystem) => AnyRef): Unit = {
    an[IllegalArgumentException] shouldBe thrownBy(func(inputPath, null))
    an[IllegalArgumentException] shouldBe thrownBy(func(null, fs))
    an[IllegalArgumentException] shouldBe thrownBy(func("", fs))
  }
}
