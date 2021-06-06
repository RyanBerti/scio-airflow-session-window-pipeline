package ryanberti

import org.joda.time.{DateTime, Instant}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FileIOUtilsTest extends AnyFlatSpec with Matchers {

  "FileIOUtils" should "convert instants to paths and vv" in {

    val instant = new DateTime().withSecondOfMinute(0).withMillisOfSecond(0).toInstant
    val path = FileIOUtils.instantToPartitionPath(instant)
    val newInstant = FileIOUtils.partitionPathToInstant(path)

    assert(instant == newInstant)

  }
}
