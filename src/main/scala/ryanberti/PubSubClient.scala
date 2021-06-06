package ryanberti

import java.util.concurrent.TimeUnit

import com.google.cloud.pubsub.v1.Publisher
import com.google.pubsub.v1.{PubsubMessage, TopicName}
import ryanberti.schema.ExampleMessage

import scala.concurrent.{ExecutionContext, Future}

/***
 * Super simple pubsub client used solely by PubSubClientTest to messages to
 * our input topic for pipeline consumption
 *
 * @param project the GCP project in which the topic lives
 * @param topic the PubSub topic to write messages to
 */
class PubSubClient(project: String, topic: String) {

  val topicName = TopicName.of(project, topic)
  val publisher = Publisher.newBuilder(topicName).build
  implicit val executionContext = ExecutionContext.global

  def publishExampleMessage(message: ExampleMessage): Future[String] = {
    val psMessage = PubsubMessage.newBuilder
    psMessage.setData(message.toByteString)
    Future { publisher.publish(psMessage.build).get() }
  }

  def shutdownPublisher(): Unit = {
    publisher.shutdown()
    publisher.awaitTermination(1, TimeUnit.MINUTES)
  }

}
