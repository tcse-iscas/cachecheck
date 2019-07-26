	
import java.io._

  /**
   * Mark this RDD for persisting using the specified level.
   *
   * @param newLevel the target storage level
   * @param allowOverride whether to override any existing level with the new one
   */
  private def persist(newLevel: StorageLevel, allowOverride: Boolean): this.type = {
    // TODO: Handle changes of StorageLevel
    if (storageLevel != StorageLevel.NONE && newLevel != storageLevel && !allowOverride) {
      throw new UnsupportedOperationException(
        "Cannot change storage level of an RDD after it was already assigned a level")
    }
    // If this is the first time this RDD is marked for persisting, register it
    // with the SparkContext for cleanups and accounting. Do this only once.
    if (storageLevel == StorageLevel.NONE) {
      sc.cleaner.foreach(_.registerRDDForCleanup(this))
      sc.persistRDD(this)
    }
    storageLevel = newLevel
    // instrumentation code
    val appname = this.conf.get("spark.app.name")
    val tracePath: String = System.getProperty("user.dir") + "\\trace\\"
    val traceFile: File = new File(tracePath + appname + ".trace")
    if(!traceFile.exists())
      traceFile.createNewFile()
    val fos = new FileOutputStream(traceFile, true)
    val osw = new OutputStreamWriter(fos)
    osw.write("persist "+ this.id + "\r\n")
    osw.close()
    // end instrumentation code
    this
  }
  
  /**
   * Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.
   *
   * @param blocking Whether to block until all blocks are deleted.
   * @return This RDD.
   */
  def unpersist(blocking: Boolean = true): this.type = {
    logInfo("Removing RDD " + id + " from persistence list")
    sc.unpersistRDD(id, blocking)
    storageLevel = StorageLevel.NONE
    // instrumentation code
    val appname = this.conf.get("spark.app.name")
    val tracePath: String = System.getProperty("user.dir") + "\\trace\\"
    val traceFile: File = new File(tracePath + appname + ".trace")
    if(!traceFile.exists())
      traceFile.createNewFile()
    val fos = new FileOutputStream(traceFile, true)
    val osw = new OutputStreamWriter(fos)
    osw.write("unpersist "+ this.id + "\r\n")
    osw.close()
    // end instrumentation code
    this
  }