package tasks

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import dcollections.api.{DistContext, RecordNumber}
import collection.immutable
import collection.mutable

/**
 * User: vjovanovic
 * Date: 4/4/11
 */

abstract class DistributedCollectionsMapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT] extends Mapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT] with CollectionTask {
  var distContext: DistContext = null

  override def setup(context: Mapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT]#Context) = {
    super.setup(context)
    val localCache = mutable.Map[String, Any]()
    val globalCache = deserializeOperation[immutable.Map[String, Any]](context.getConfiguration, "global.cache")
    distContext = new DistContext(localCache, globalCache.get)
  }

  override def run(context: Mapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT]#Context) = {
    setup(context)

    val fileSplit = context.getInputSplit.asInstanceOf[FileSplit]
    val fileNameParts = fileSplit.getPath().getName.split("-")

    val fileNumber = Integer.parseInt(fileNameParts(fileNameParts.length - 1))
    val recordStart = fileSplit.getStart
    distContext.recordNumber = new RecordNumber(fileNumber, recordStart, 0L)

    while (context.nextKeyValue()) {
      distMap(context.getCurrentKey(), context.getCurrentValue(), context, distContext)
      distContext.recordNumber.incrementRecordCounter()
    }

    cleanup(context)
  }

  def distMap(key: KEYIN, value: VALUEIN, context: Mapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT]#Context, distContext: DistContext)
}
