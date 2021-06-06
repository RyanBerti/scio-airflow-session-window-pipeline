package ryanberti

import com.spotify.scio.ScioContext
import com.spotify.scio.values.WindowOptions
import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.transforms.windowing.{AfterProcessingTime, AfterWatermark, Repeatedly}
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration

/***
 * Simple helper object that adds some extra triggers to the
 * pipeline's WindowOptions when we're running with the DirectRunner
 */
object WindowOptionsUtils {

  def isDirectRunner(implicit sc: ScioContext): Boolean = {
    sc.options.getRunner == classOf[DirectRunner]
  }

  def sessionWindowOptions(implicit sc: ScioContext): WindowOptions = {
    val trigger = if (isDirectRunner) {
      AfterWatermark.pastEndOfWindow()
        // DirectRunner doesn't seem to have the ability to determine watermark from unbounded source,
        // so adding the early firing is the only way to get some output when running locally
        .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(1)))
        .withLateFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(1)))
    } else {
      AfterWatermark.pastEndOfWindow()
        .withLateFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(1)))
    }

    WindowOptions(
      trigger = trigger,
      accumulationMode = AccumulationMode.ACCUMULATING_FIRED_PANES,
      allowedLateness = Duration.standardDays(1)
    )
  }

  def fixedWindowOptions(implicit sc: ScioContext): WindowOptions = {
    val trigger = if (isDirectRunner) {
      Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(1)))
    } else {
      null
    }

    WindowOptions(
      trigger = trigger
    )
  }

}
