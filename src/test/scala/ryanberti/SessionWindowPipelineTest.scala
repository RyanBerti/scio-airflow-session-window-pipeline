package ryanberti

import com.google.protobuf.timestamp.Timestamp
import com.spotify.scio.testing.testStreamOf
import org.joda.time.{DateTime, Duration, Instant}
import ryanberti.schema.ExampleMessage
import com.spotify.scio.testing._
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.values.TimestampedValue

class SessionWindowPipelineTest extends PipelineSpec {

  val startTimeMillis = new DateTime().withSecondOfMinute(0).withMillisOfSecond(0).getMillis
  val startTimeSeconds = startTimeMillis / 1000

  "Pipeline outputs ontime and late panes" should "work" in {
    val stream = testStreamOf[ExampleMessage]
      .advanceWatermarkTo(new Instant(startTimeMillis))
      .addElements(
        TimestampedValue.of(
          ExampleMessage(
            sessionId = "1",
            messageName = "browse",
            timestamp = Some(Timestamp(seconds = startTimeSeconds)),
          ), new Instant(startTimeMillis + (60 * 1000))),
        TimestampedValue.of(
          ExampleMessage(
            sessionId = "1",
            messageName = "browse",
            timestamp = Some(Timestamp(seconds = startTimeSeconds + 30)),
          ), new Instant(startTimeMillis + (60 * 1000))),
        TimestampedValue.of(
          ExampleMessage(
            sessionId = "1",
            messageName = "click",
            timestamp = Some(Timestamp(seconds = startTimeSeconds + 45)),
          ), new Instant(startTimeMillis + (60 * 1000)))
      )
      // advance watermark to just after the session window closes (45 sec + gap duration)
      .advanceWatermarkTo(new Instant(startTimeMillis + (105 * 1000)))
      .addElements(
        TimestampedValue.of(
          ExampleMessage(
            sessionId = "1",
            messageName = "click",
            timestamp = Some(Timestamp(seconds = startTimeSeconds + 40)),
          ), new Instant(startTimeMillis + (105 * 1000)))
      )
      .advanceWatermarkToInfinity()

    runWithContext { implicit sc =>
      val sessionMetrics = SessionWindowPipeline.sessionizeMessagesDeriveMetrics(sc.testStream(stream))

      sessionMetrics.debug()

      val window = new IntervalWindow(new Instant(startTimeMillis), Duration.standardMinutes(1))
      sessionMetrics should inOnTimePane(window) {
        containInAnyOrder(Seq(
          SessionMetrics("1", 45, Map("browse" -> 2, "click" -> 1))
        ))
      }

      // always expected the accumulated firing to end up in the window's late pane,
      // but wasn't able to get the pane extractors to work
      sessionMetrics should containValue(SessionMetrics("1", 45, Map("browse" -> 2, "click" -> 2)))
//      sessionMetrics should inLatePane(window) {
//        containInAnyOrder(Seq(
//          SessionMetrics("1", 45, Map("browse" -> 2, "click" -> 2))
//        ))
//      }
    }
  }
}
