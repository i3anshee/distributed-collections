package io

import dcollections.api.io.{CollectionMetaData, CollectionsIOAPI}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import dcollections.api.CollectionId
import java.io.{ObjectInputStream}

/**
 * User: vjovanovic
 * Date: 4/11/11
 */

object HadoopDFSIO extends CollectionsIOAPI {

  def getCollectionMetaData(id: CollectionId): CollectionMetaData = {
    val conf = new Configuration()
    val fs = FileSystem.get(id.location, conf)
    var in: Option[ObjectInputStream] = None
    try {
      in = Some(new ObjectInputStream(fs.open(new Path(id.location.toString, "META"))))

      return in.get.readObject.asInstanceOf[CollectionMetaData]
    } finally {
      if (in.isDefined) in.get.close
    }
  }

}