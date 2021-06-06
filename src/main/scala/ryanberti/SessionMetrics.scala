package ryanberti

import org.slf4j.Logger
import ryanberti.schema.ExampleMessage

/***
 * This is an example case class that we will use to derive metrics about messages within the session
 * This case class will be used to define the schema of the output parquet file
 *
 * @param sessionId
 * @param sessionLengthSeconds
 * @param messageCount
 */
case class SessionMetrics(sessionId: String, sessionLengthSeconds: Int, messageCount: Map[String, Int])

object SessionMetrics {
  def fromSessionizedMessages(sessionMessages: (String, Iterable[ExampleMessage]))(implicit logger: Logger): (Long, SessionMetrics) = {
    val (sessionId, messageList) = sessionMessages

    logger.info("Transforming events for session: " + sessionId)

    //get min/max time for all messages in the session to derive session length
    val (min, max) = messageList.map(_.timestamp.get.seconds).foldLeft((Int.MaxValue, Int.MinValue))({
      case ((min, max), cur) => (Math.min(min, cur).toInt, Math.max(max, cur).toInt)
    })

    //build a simple map of message type counts
    val messageCounts = messageList.groupBy(_.messageName).map({
      case (messageName, messages) => messageName -> messages.size
    })

    (min, SessionMetrics(sessionId, max - min, messageCounts))
  }
}
