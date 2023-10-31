package com.amadeus.dataio.core

import org.apache.commons.io.{FilenameUtils, IOUtils}
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.{ByteArrayOutputStream, FileNotFoundException}
import java.util.zip.{ZipEntry, ZipOutputStream}

/**
 * Contains a variety of helper functions to facilitate
 * reading files.
 */
trait FileReaderHelper {

  /**
   * Returns the content of a file as Array[Byte].
   * @param path The path of the file to read from.
   * @param fs The FileSystem where the file is stored.
   * @return The file as an array of bytes.
   * @throws FileNotFoundException if the file does not exist.
   * @throws IllegalArgumentException if any of the arguments is nnull or empty.
   * @throws java.io.IOException if an I/O error occurs.
   */
  def toByteArray(path: String, fs: FileSystem): Array[Byte] = {
    if (fs == null)
      throw new IllegalArgumentException("Can not read a file with a null filesystem.")
    IOUtils.toByteArray(fs.open(new Path(path)))
  }

  /**
   * Returns the content of a file as String.
   * @param path The path of the file to read from.
   * @param fs The FileSystem where the file is stored.
   * @param charsetName The name of the requested charset, null means platform default
   * @return The file as String.
   * @throws FileNotFoundException if the file does not exist.
   * @throws IllegalArgumentException if any of the arguments is null or empty.
   * @throws java.io.IOException if an I/O error occurs.
   */
  def toString(path: String, fs: FileSystem, charsetName: String = null): String = {
    if (fs == null)
      throw new IllegalArgumentException("Can not read a file with a null filesystem.")
    IOUtils.toString(fs.open(new Path(path)), charsetName)
  }

  /**
   * Zips a file and returns it as Array[Byte].
   * @param path the path of the file to zip.
   * @param fs The FileSystem where the file is stored.
   * @return The byte array equivalent of the file
   * @throws FileNotFoundException if the file does not exist.
   * @throws IllegalArgumentException if any of the arguments is nnull or empty.
   * @throws java.io.IOException if an I/O error occurs.
   */
  def toZipByteArray(path: String, fs: FileSystem): Array[Byte] = {
    val name    = FilenameUtils.getName(path)
    val content = toByteArray(path, fs)

    val outputStream    = new ByteArrayOutputStream
    val zipOutputStream = new ZipOutputStream(outputStream)
    val entry           = new ZipEntry(name)
    entry.setSize(content.length)
    zipOutputStream.putNextEntry(entry)
    zipOutputStream.write(content)
    zipOutputStream.closeEntry()
    zipOutputStream.close()

    outputStream.toByteArray
  }
}

/**
 * Contains a variety of helper functions to facilitate
 * reading files.
 */
object FileReaderHelper extends FileReaderHelper
