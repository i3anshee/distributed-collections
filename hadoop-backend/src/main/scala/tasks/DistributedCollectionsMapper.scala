package tasks

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import java.util.UUID
import org.apache.hadoop.fs.Path
import dcollections.api.RecordNumber

/**
 * User: vjovanovic
 * Date: 4/4/11
 */

abstract class DistributedCollectionsMapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT] extends Mapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT] {
  val inputCollectionsMap: Map[UUID, Path] = Map()

  override def run(context: Mapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT]#Context) = {
    setup(context);
    val fileSplit = context.getInputSplit.asInstanceOf[FileSplit]
    val fileNameParts = fileSplit.getPath().getName.split("-")

    val fileNumber = Integer.parseInt(fileNameParts(fileNameParts.length - 1))
    val recordStart = fileSplit.getStart
    //    val currentCollectionUUID: UUID = inputCollectionsMap.get(fileSplit.getPath.getParent)
    var counter = 0L
    val record = new RecordNumber(fileNumber, recordStart, counter)

    while (context.nextKeyValue()) {
      distMap(context.getCurrentKey(), context.getCurrentValue(), context, record)
      record.nextRecord()
    }

    //    assert(fileSplit.getLength == counter, "Number of processed records must be the same as number of records in split.")
    cleanup(context);
  }

  def distMap(key: KEYIN, value: VALUEIN, context: Mapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT]#Context, record: RecordNumber)
}
