package com.amadeus.dataio.config.fields

import com.typesafe.config.Config
import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.duration.Duration
import scala.util.Try

/** Retrieve the trigger to be used from the configuration.
  *
  * See documentation: [[https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#continuous-processing]]
  */
trait StreamingTriggerConfigurator {

  /** @param config The typesafe Config object holding the configuration.
    * @return The trigger, or None.
    * @throws IllegalArgumentException in case the combination of trigger and duration is not supported.
    */
  def getStreamingTrigger(implicit config: Config): Option[Trigger] = {
    val streamingTrigger = Try(config.getString("trigger")).toOption
    val duration         = Try(config.getString("duration")).toOption

    (streamingTrigger, duration) match {
      case (Some("AvailableNow"), _)            => Some(Trigger.AvailableNow())
      case (Some("Continuous"), Some(duration)) => Some(Trigger.Continuous(Duration(duration)))
      case (None, Some(duration))               => Some(Trigger.ProcessingTime(Duration(duration)))
      case (None, None)                         => None
      case _ =>
        throw new IllegalArgumentException(
          s"The couple ($streamingTrigger, $duration) is not part of the allowed streaming trigger values"
        )
    }
  }
}
