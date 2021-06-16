package ryanberti

import com.spotify.scio.coders.Coder
import com.spotify.scio.{ContextAndArgs, ScioContext}
import com.spotify.scio.pubsub._
import com.spotify.scio.values.SCollection
import org.joda.time.{DateTime, DateTimeZone, Duration, Instant}
import org.apache.beam.sdk.options.StreamingOptions
import magnolify.avro.AvroType
import org.apache.beam.sdk.transforms.ParDo
import org.slf4j.LoggerFactory
import ryanberti.Schema.ExampleMessage
import io.circe.syntax._

object SessionWindowPipeline {

  implicit val logger = LoggerFactory.getLogger(this.getClass)

  def main(cmdlineArgs: Array[String]): Unit = {

    // Read args, flag the job as a streaming job
    implicit val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.optionsAs[StreamingOptions].setStreaming(true)

    // Read proto ExampleMessages from pubsub subscription
    val inputIO = PubsubIO.proto[ExampleMessage](name = args.required("input-subscription"))
    val readParam = PubsubIO.ReadParam(PubsubIO.Subscription)
    val messages = sc.read(inputIO)(readParam)

    // Beam's PubsubIO expects java proto messages, so we read java and convert to scala
    // https://spotify.github.io/scio/io/Protobuf.html#scalapb
    var scalaMessages = messages.map(schema.ExampleMessage.fromJavaProto)

    // Log the messages' event timestamps
    scalaMessages = scalaMessages.map(m => {
      logger.info("Parsed message with event time " + new DateTime(m.timestamp.get.seconds * 1000).withZone(DateTimeZone.UTC))
      m
    })

    // Apply sessionization via session window, derive SessionMetrics
    val sessionMetrics = sessionizeMessagesDeriveMetrics(scalaMessages)

    // Setup Avro schema and coder in prep for writing
    val avroType = AvroType[SessionMetrics]
    implicit val coder = Coder.avroGenericRecordCoder(avroType.schema)

    // Map to GenericRecords and use Beam API to write with FileIO
    // so we have access to the output filenames
    val fileNames = sc.wrap(sessionMetrics.map(avroType.to).internal.apply(
        FileIOUtils.forTarget(args.required("output-destination"), avroType.schema)
      ).getPerDestinationOutputFilenames).map(kv => kv.getValue)

    // Convert filenames to event time instants map entries, write them in json
    // to the output topic. This is where we could de-dupe/throttle/filter as-needed.
    val json = fileNames
      .map(fn => Map("event_time_second" -> FileIOUtils.partitionPathToInstant(fn).getMillis / 1000))
      .map(m => m.asJson.noSpaces)

    json.write(PubsubIO.string(args.required("output-topic")))(PubsubIO.WriteParam(None, None))

    sc.run().waitUntilFinish()
  }

  // Extracted core logic into a function to support testing
  def sessionizeMessagesDeriveMetrics(scalaMessages: SCollection[schema.ExampleMessage])(implicit sc: ScioContext): SCollection[SessionMetrics] = {

    // Re-associate messages with their event timestamps,
    // set allowed skew to handle writing backwards in time
    val tsMessages = scalaMessages.timestampBy(m => Instant.ofEpochSecond(m.timestamp.get.seconds), Duration.standardDays(1))

    // Sessionize messages; for info on session windows, watermarks, accumulation and triggering see
    // https://beam.apache.org/documentation/programming-guide/#watermarks-and-late-data
    // https://beam.apache.org/documentation/programming-guide/#window-accumulation-modes
    // https://beam.apache.org/documentation/programming-guide/#event-time-triggers
    val sessionizedMessages = tsMessages.withSessionWindows(
        gapDuration = Duration.standardMinutes(1),
        options = WindowOptionsUtils.sessionWindowOptions
      )
      .groupBy(_.sessionId)

    // Derived metrics along with session start time using the messages within each session
    val sessionMetricTuples = sessionizedMessages.map(SessionMetrics.fromSessionizedMessages)

    // This is the behavior I'd assume from the TimestampCombiner.EARLIEST window option
    var sessionMetrics = sessionMetricTuples.timestampBy(smt => Instant.ofEpochSecond(smt._1), Duration.standardDays(1)).map(_._2)

    // Use DoFn to log the timestamp associated with each SessionMetrics element
    sessionMetrics = sc.wrap(sessionMetrics.internal.apply(
      ParDo.of(new TimestampLoggerDoFn[SessionMetrics](logger, "Handling SessionMetrics with timestamp: %s, window: %s"))
    ))

    // Re-window the derived records in prep for writing event time partitions
    sessionMetrics.withFixedWindows(
      Duration.standardMinutes(1),
      options = WindowOptionsUtils.fixedWindowOptions
    )
  }

}
