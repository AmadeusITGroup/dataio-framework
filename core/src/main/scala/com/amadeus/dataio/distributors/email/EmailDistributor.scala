package com.amadeus.dataio.distributors.email

import com.amadeus.dataio.core.{Distributor, FileReaderHelper, Logging}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.FileSystem

import java.util.Properties
import javax.activation.DataHandler
import javax.mail._
import javax.mail.internet.{InternetAddress, MimeBodyPart, MimeMessage, MimeMultipart}
import javax.mail.util.ByteArrayDataSource
import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Allows to send files by email.
 * @param session The javax.mail.session to use to send the email.
 * @param fromEmail The email address of the sender.
 * @param toEmails The sequence of recipients.
 * @param ccEmails Carbon copy sequence.
 * @param bccEmails Blind carbon copy sequence.
 * @param subject The subject of the email to send.
 * @param textBody The content of the email.
 * @param compressAttachments Whether attachments should be zipped or not.
 * @param config Contains the Typesafe Config object that was used at instantiation to configure this entity.
 */
case class EmailDistributor(
    session: Session,
    fromEmail: String,
    toEmails: Seq[String],
    ccEmails: Seq[String],
    bccEmails: Seq[String],
    subject: String,
    textBody: String,
    compressAttachments: Boolean,
    config: Config = ConfigFactory.empty()
) extends Distributor
    with Logging {

  /**
   * Sends an email with the received file as attachment.
   * @param path The path of the file to send.
   * @param fs The FileSystem on which the file is stored.
   */
  def send(path: String)(implicit fs: FileSystem): Unit = {
    val fileName = FilenameUtils.getName(path)
    val message  = prepareMessage(path)

    logger.info(s"Sending $fileName by email.")
    try {
      Transport.send(message)
      logger.info(s"Email with report $path sent successfully!")
    } catch {
      case ex: MessagingException =>
        logger.error(s"Issue while sending $fileName by email! Error is: ${ex.toString}.")
        throw ex
    }
  }

  /**
   * Prepares a message with the report to send attached.
   * @param path The path of the file to attach to the message.
   * @param fs The FileSystem on which the file is stored.
   * @return a new MimeMessage with the report attached.
   */
  private def prepareMessage(path: String)(implicit fs: FileSystem) = {
    val message = new MimeMessage(session)

    message.addHeader("Content-type", "text/HTML; charset=UTF-8")
    message.addHeader("format", "flowed")
    message.addHeader("Content-Transfer-Encoding", "8bit")

    message.setFrom(new InternetAddress(fromEmail, "NoReply-JD"))
    message.setSubject(subject, "UTF-8")

    message.setRecipients(Message.RecipientType.TO, toEmails.mkString(","))
    message.setRecipients(Message.RecipientType.CC, ccEmails.mkString(","))
    message.setRecipients(Message.RecipientType.BCC, bccEmails.mkString(","))

    val multipart = new MimeMultipart()

    val textPart: MimeBodyPart = new MimeBodyPart()
    textPart.setText(textBody, "utf-8")
    multipart.addBodyPart(textPart)

    val attachment: MimeBodyPart = attachFile(path)
    multipart.addBodyPart(attachment)

    message.setContent(multipart)
    message
  }

  /**
   * Generates an attachment for a MimeMessage from a file, compressed or not.
   * @param path The path of the file to include.
   * @param fs The FileSystem on which the file is stored.
   * @return a MimeBodyPart containing the file.
   */
  private def attachFile(path: String)(implicit fs: FileSystem): MimeBodyPart = {
    logger.info(s"Adding $path as attachment.")
    val attachment: MimeBodyPart = new MimeBodyPart()
    val filename                 = FilenameUtils.getName(path)

    if (compressAttachments) {
      logger.info(s"Compressing $path beforehand.")
      val zipByteArray = FileReaderHelper.toZipByteArray(path, fs)
      attachment.setDataHandler(new DataHandler(new ByteArrayDataSource(zipByteArray, "text/csv")))
      attachment.setFileName(s"$filename.zip")
    } else {
      val fileByteArray = FileReaderHelper.toByteArray(path, fs)
      attachment.setDataHandler(new DataHandler(new ByteArrayDataSource(fileByteArray, "text/csv")))
      attachment.setFileName(filename)
    }

    attachment
  }
}

object EmailDistributor {

  /**
   * <p>Creates a new instance of EmailDistributor from a typesafe Config object.</p>
   * It must contain the following fields:
   * <ul>
   * <li>SmtpUser: String</li>
   * <li>SmtpPassword: String</li>
   * <li>To: List[String]</li>
   * <li>Subject: String</li>
   * </ul>
   *
   * The following fields are optional:
   * <ul>
   * <li>From: String</li>
   * <li>Cc: List[String]</li>
   * <li>Bcc: List[String]</li>
   * <li>Body: String</li>
   * <li>Compressed: Boolean</li>
   * </ul>
   * @param config typesafe Config object containing the configuration fields.
   * @return a new EmailDistributor object.
   * @throws com.typesafe.config.ConfigException If any of the mandatory config is not available in the config argument.
   */
  def apply(config: Config): EmailDistributor = {
    val smtpHost            = config.getString("SmtpHost")
    val smtpUser            = config.getString("SmtpUser")
    val smtpPassword        = config.getString("SmtpPassword")
    val fromEmail           = config.getString("From")
    val toEmails            = config.getStringList("To").asScala
    val ccEmails            = Try { config.getStringList("Cc").asScala }.getOrElse(Nil)
    val bccEmails           = Try { config.getStringList("Bcc").asScala }.getOrElse(Nil)
    val subject             = config.getString("Subject")
    val textBody            = Try { config.getString("Body") }.getOrElse("Please find attached the wished report.")
    val compressAttachments = Try { config.getBoolean("Compressed") }.getOrElse(false)

    val session = makeSession(smtpHost, smtpUser, smtpPassword)

    new EmailDistributor(
      session,
      fromEmail,
      toEmails,
      ccEmails,
      bccEmails,
      subject,
      textBody,
      compressAttachments,
      config
    )
  }

  /**
   * Creates a new session object including an authenticator based on SMTP authentication information
   * @param smtpUser The SMTP username to use in the authenticator
   * @param smtpPassword The SMTP password to use in the authenticator
   * @return The new session
   */
  private def makeSession(smtpHost: String, smtpUser: String, smtpPassword: String): Session = {
    val props = new Properties()
    props.put("mail.transport.protocol", "smtp")
    props.put("mail.smtp.host", smtpHost)
    props.put("mail.smtp.auth", "true")
    props.put("mail.smtp.user", smtpUser)

    val authenticator = new Authenticator {
      override def getPasswordAuthentication: PasswordAuthentication = new PasswordAuthentication(smtpUser, smtpPassword)
    }

    val session: Session = Session.getInstance(props, authenticator)
    session
  }
}
