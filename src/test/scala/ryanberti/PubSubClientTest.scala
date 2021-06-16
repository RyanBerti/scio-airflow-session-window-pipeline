package ryanberti


import com.google.protobuf.timestamp.Timestamp
import org.joda.time.{DateTime, DateTimeZone, Duration}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ryanberti.schema.ExampleMessage

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}
import scala.language.postfixOps

class PubSubClientTest extends AnyFlatSpec with Matchers {

  val gcpProject = "<your GCP project>"
  val pubSubTopic = "<your PS topic>"

  // run this manually to write example messages to a real topic
  // to test the pipeline end-to-end
  ignore should "write messages within the same session" in {

    val psClient = new PubSubClient(gcpProject, pubSubTopic)

    val sessionStartSeconds = DateTime.now()
      .withSecondOfMinute(0)
      .withMillisOfSecond(0)
      .minus(Duration.standardHours(1)).getMillis / 1000

    val browseOne = ExampleMessage(
      timestamp = Some(Timestamp(seconds = sessionStartSeconds)),
      sessionId = "1",
      messageName = "browse")

    val browseTwo = ExampleMessage(
      timestamp = Some(Timestamp(seconds = sessionStartSeconds + 30)),
      sessionId = "1",
      messageName = "browse")

    val clickOne = ExampleMessage(
      timestamp = Some(Timestamp(seconds = sessionStartSeconds + 40)),
      sessionId = "1",
      messageName = "click")

    val clickTwo = ExampleMessage(
      timestamp = Some(Timestamp(seconds = sessionStartSeconds + 45)),
      sessionId = "1",
      messageName = "click")

    implicit val ec = ExecutionContext.global
    Seq(browseOne, browseTwo, clickOne, clickTwo).map(psClient.publishExampleMessage).foreach(f => Await.result(f.map(println), 10 seconds))

    psClient.shutdownPublisher()

  }

  // run this manually to write example messages to a real topic
  // to test the pipeline end-to-end
  ignore should "write messages within the same session with lateness" in {

    val psClient = new PubSubClient(gcpProject, pubSubTopic)

    val sessionStartSeconds = DateTime.now()
      .withSecondOfMinute(0)
      .withMillisOfSecond(0)
      .minus(Duration.standardHours(1)).getMillis / 1000

    val browseOne = ExampleMessage(
      timestamp = Some(Timestamp(seconds = sessionStartSeconds)),
      sessionId = "2",
      messageName = "browse")

    val browseTwo = ExampleMessage(
      timestamp = Some(Timestamp(seconds = sessionStartSeconds + 30)),
      sessionId = "2",
      messageName = "browse")

    val clickOne = ExampleMessage(
      timestamp = Some(Timestamp(seconds = sessionStartSeconds + 40)),
      sessionId = "2",
      messageName = "click")

    val clickTwo = ExampleMessage(
      timestamp = Some(Timestamp(seconds = sessionStartSeconds + 45)),
      sessionId = "2",
      messageName = "click")

    implicit val ec = ExecutionContext.global

    Seq(browseOne, browseTwo, clickTwo).map(psClient.publishExampleMessage).foreach(f => Await.result(f.map(println), 10 seconds))

    println("Sleeping for 1 minute 10 sec")
    Thread.sleep(70 * 1000)

    Seq(clickOne).map(psClient.publishExampleMessage).foreach(f => Await.result(f.map(println), 10 seconds))

  }

}
