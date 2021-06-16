package ryanberti

import java.io.File

import com.google.protobuf.Timestamp
import com.spotify.scio.parquet.avro.ParquetAvroIO
import com.spotify.scio.pubsub.PubsubIO
import com.spotify.scio.testing.PipelineSpec
import org.joda.time.DateTime
import ryanberti.Schema.ExampleMessage
import io.circe.syntax._

import scala.reflect.io.Directory

class SessionWindowPipelineJobTest extends PipelineSpec {

  val startTime = DateTime.now().getMillis / 1000

  val inData: Seq[ExampleMessage] = Seq(
    ExampleMessage.newBuilder()
      .setSessionId("1")
      .setMessageName("browse")
      .setTimestamp(Timestamp.newBuilder().setSeconds(startTime))
      .build(),
    ExampleMessage.newBuilder()
      .setSessionId("1")
      .setMessageName("browse")
      .setTimestamp(Timestamp.newBuilder().setSeconds(startTime + 30))
      .build(),
    ExampleMessage.newBuilder()
      .setSessionId("1")
      .setMessageName("click")
      .setTimestamp(Timestamp.newBuilder().setSeconds(startTime + 45))
      .build()
  )

  val expectedOutputs: Seq[SessionMetrics] = Seq(
    SessionMetrics("1", 45, Map("browse" -> 2, "click" -> 1)),
  )

  val expectedEventTimes = Seq(Map("event_time_second" -> new DateTime(startTime * 1000).withSecondOfMinute(0).getMillis / 1000).asJson.noSpaces)

  "Simple Late Arriving Data Example" should "work" in {
    JobTest[SessionWindowPipeline.type]
      .args("--input-subscription=subscription", "--output-destination=path", "--output-topic=topic")
      .input(PubsubIO.proto[ExampleMessage]("subscription"), inData)
      // unfortunately, scio doesn't recognize the implementations beam transform to write
      // files and return file names; maybe taps supports this?
      // .output(ParquetAvroIO[SessionMetrics]("path"))(outputs => outputs should containInAnyOrder(expectedOutputs))
      .output(PubsubIO.string("topic"))(eventTimes => eventTimes should containInAnyOrder(expectedEventTimes))
      .run()
  }

  "JobTest" should "have it outputs cleaned up" in {
    println("Removing " + System.getProperty("user.dir") + "/path")
    new Directory(new File(System.getProperty("user.dir") + "/path")).deleteRecursively()
  }
}
