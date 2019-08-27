
import java.io._
import scala.collection.mutable.{ArrayBuffer}

  // instrumentation code
  private def jobInfo(rdd: RDD[_]): String = {

    def traverseChildren(rdd: RDD[_]): Seq[String] = {
      val len = rdd.dependencies.length
      len match{
        case 0 => Seq.empty
        case 1 =>
          val childrdd = rdd.dependencies.head.rdd
          val childrddid = childrdd.id
          (rdd.id +"-"+childrddid) +: traverseChildren(childrdd)
        case _ =>
          rdd.dependencies.flatMap(d => ((rdd.id + "-" + d.rdd.id) +: traverseChildren(d.rdd)))
      }
    }

    traverseChildren(rdd).mkString(",")
  }

  private def getJobNums(): Int = {
    val appname = this.conf.get("spark.app.name")
    val tracePath: String = System.getProperty("user.dir") + "\\trace\\"
    val traceFile: File = new File(tracePath + appname + ".trace")
    if(!traceFile.exists())
      return 0
    val fis = new FileInputStream(traceFile)
    val br = new BufferedReader(new InputStreamReader(fis))
    var str = br.readLine()
    var result = 0
    while (str != null) {
      if(str.startsWith("job"))
        result += 1
      str = br.readLine()
    }
    result
  }

  
  def writeRDDInfo(rdd: RDD[_], infoFilePath: String): Unit = {

    val infoFile = new File(infoFilePath)
    val rddIdIndex = new ArrayBuffer[Int]()
    try {

      // Read the rddInfoFile head
      if (infoFile.exists) {
        val header = new BufferedReader(new InputStreamReader(new FileInputStream(infoFile), "UTF-8"))
        val fileHeader = header.readLine()
        if (!((null == fileHeader) || fileHeader.equals(""))) {
          val rddIdInHedaer = fileHeader.split(":").last.split(",")
          rddIdInHedaer.foreach(rddId => {
            rddIdIndex += rddId.toInt
          })
        }
        header.close
      } else
        infoFile.createNewFile

      // write new rdd info
      val out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(infoFile,true), "UTF-8"))

      def traverseChildren(rdd: RDD[_], outStream: BufferedWriter): Unit = {
        val len = rdd.dependencies.length
        len match{
          case 0 => Seq.empty
          case 1 =>
            val childrdd = rdd.dependencies.head.rdd
            val childrddid = childrdd.id
            traverseChildren(childrdd, outStream)
          case _ =>
            val children = rdd.dependencies
            children.foreach(child => {
              traverseChildren(child.rdd, outStream)
            })
        }
        if(!rddIdIndex.contains(rdd.id)) {
          rddIdIndex += rdd.id
          val fullcallsite = rdd.creationSite.longForm.split("\n")
          outStream.write(s"$rdd" + " || ")
          val sitelength = fullcallsite.length
          if (sitelength > 2) {
            for(i <- 2 until (sitelength - 1)) {
              outStream.write("at " + fullcallsite(i) + " || ")
            }
          }
          outStream.write("\r\n" + "\r\n")
        }
      }
      traverseChildren(rdd, out)
      out.flush
      out.close

      // Rewrite the header
      val tmpFile = File.createTempFile(infoFilePath,".tmp")
      val in = new BufferedReader(new InputStreamReader(new FileInputStream(infoFile), "UTF-8"))
      val headerRewriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tmpFile, false), "UTF-8"))
      var newHeader = new StringBuffer
      rddIdIndex.foreach(rddId => newHeader.append(rddId + ","))
      var tmpLine = ""
      tmpLine = in.readLine()
      if (null == tmpLine)
        return ""
      else {
        headerRewriter.write("IDs:"+newHeader.toString + "\r\n")
        if(tmpLine.startsWith("IDs"))
          tmpLine = in.readLine()
        while (null != tmpLine) {
          headerRewriter.write(tmpLine + "\r\n")
          tmpLine = in.readLine()
        }
      }
      headerRewriter.flush()
      headerRewriter.close()
      in.close()
      infoFile.delete()
      tmpFile.renameTo(infoFile)
    } catch {
      case e: IOException => {
        e.printStackTrace()
      }
    }
  }
  // end instrumentation code

  /**
   * Run a function on a given set of partitions in an RDD and pass the results to the given
   * handler function. This is the main entry point for all actions in Spark.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   * partitions of the target RDD, e.g. for operations like `first()`
   * @param resultHandler callback to pass each result to
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit): Unit = {
    if (stopped.get()) {
      throw new IllegalStateException("SparkContext has been shutdown")
    }

    // instrumentation code
    val appname = this.conf.get("spark.app.name")
    val tracePath: String = System.getProperty("user.dir") + "\\trace\\"

    println("Trace dir: " + tracePath)
    writeRDDInfo(rdd, tracePath + appname+".info")

    val traceFile: File = new File(tracePath + appname + ".trace")
    val jobFile: File = new File(tracePath + appname + ".job")
    val jobNum = getJobNums()
    if(!traceFile.exists())
      traceFile.createNewFile()
    val fos = new FileOutputStream(traceFile, true)
    val osw = new OutputStreamWriter(fos)
    osw.write("job " + jobNum + "\r\n")
    osw.close()

    if(!jobFile.exists())
      jobFile.createNewFile()
    val fos1 = new FileOutputStream(jobFile, true)
    val osw1 = new OutputStreamWriter(fos1)
    osw1.write("action "+ jobNum + "-" + rdd.id + "," + jobInfo(rdd) + "\r\n")
    osw1.close()
    // end instrumentation code

    val callSite = getCallSite
    val cleanedFunc = clean(func)
    logInfo("Starting job: " + callSite.shortForm)
    if (conf.getBoolean("spark.logLineage", false)) {
      logInfo("RDD's recursive dependencies:\n" + rdd.toDebugString)
    }
    dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get)
    progressBar.foreach(_.finishAll())
    rdd.doCheckpoint()
  }
