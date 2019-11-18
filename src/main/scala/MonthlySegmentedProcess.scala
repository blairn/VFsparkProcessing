import AreaToRTOConverter.converter
import org.apache.spark.sql.SparkSession
import com.github.nscala_time.time.Imports.{DateTime, _}

final case class Row3G(
  origimei: String,
  starttime: String,
  imsi:String,
  msisdn:String,
  currentsac:String,
  h: String,
  dt: String
) {
  def this( origimei: String,
            starttime: String,
            imsi:String,
            msisdn:String,
            currentsac:String
          ) = this(origimei, starttime, imsi, msisdn, currentsac,
    DateTime.parse(starttime).hourOfDay().roundFloorCopy().toString("HH"),
    DateTime.parse(starttime).monthOfYear().roundFloorCopy().toString("yyyy-MM-dd")
  )
}


final case class DeviceReadRow(
                        imei: String,
                        starttime: String,
                        imsi:String,
                        msisdn:String,
                        area:String,
                        h: Int,
                        dt: String,
                        fakeFrom: String = null,
                        segment: String = null,
                        rto: Integer = null
                      ) {
  def this( imei: String,
            starttime: String,
            imsi:String,
            msisdn:String,
            area:String
          ) = this(imei, starttime, imsi, msisdn, area,
    DateTime.parse(starttime).hourOfDay().roundFloorCopy().toString("HH").toInt,
    DateTime.parse(starttime).monthOfYear().roundFloorCopy().toString("yyyy-MM-dd")
  )

  def international() = !imsi.startsWith("530")

  def this(row3g:Row3G) = this(row3g.origimei,row3g.starttime,row3g.imsi,row3g.msisdn,row3g.currentsac,row3g.h.toInt,row3g.dt)

}




object MonthlySegmentedProcess {

  def getEarliestRow(x: DeviceReadRow, y: DeviceReadRow): DeviceReadRow = {
    val xIsFake = x.fakeFrom != null
    val yIsFake = y.fakeFrom != null

    // prefer real over fake
    if (!xIsFake && yIsFake) {
      return x
    }

    // prefer real over fake
    if (xIsFake && !yIsFake) {
      return y
    }

    // prefer newest fake over old fake
    if (xIsFake && yIsFake) {
      return if (x.fakeFrom > y.fakeFrom) x else y // latest decay
    }

    if (x.starttime < y.starttime) x else y
  }


  // returns original + 6 hours of decayed readings.
  def makeFakes(v: DeviceReadRow) = {
    (0 to 6).map(x => if (x==0) v else v.copy(starttime = (DateTime.parse(v.starttime) + x.hours).toString, fakeFrom = v.starttime))
  }

  final case class ImeiAreaCount(imei:String, area:String, count:Int )

  def homeAreaCount(k: (String,String), v: Int): ImeiAreaCount = ImeiAreaCount(k._1, k._2, v)


  // biggestest counter, except internation sims win, because they are likely to change at the airport
  def biggestCount(a: ((String, String), Long), b: ((String, String), Long)): ((String, String), Long) = {
    // ((imei, rto), count)
    if (a._1._2 == "international") return a
    if (b._1._2 == "international") return b

    if (a._2 > b._2) a else b
  }

  final case class ImeiHome(imei:String, rto:Int) {
    import AreaToRTOConverter.converter
    def this(imei:String, area:String) = this(imei, converter(area))
  }

  val r = new scala.util.Random

  val imeis = converter.keySet.toVector
  val imei_size:Int = imeis.size

  def generateRandomRow3G(): Row3G = {
    import AreaToRTOConverter.converter
    val imeiNumber = r.nextInt(10000);

    val imsi = if (imeiNumber % 5 == 0) "500imsi" + imeiNumber else  "530imsi" + imeiNumber

    new Row3G(
      imeiNumber + "imei",
      (DateTime.now() + r.nextInt(10000).hours).toString,
      imsi,
      "",
      imeis(r.nextInt(imei_size))
    )
  }

  case class ImeiMonthly(imei:String, rto:Int, dt:String)

  case class MonthlyOut(rto:Int, month:String, count:Long)

  case class ImeiMonthlyResult(dt:String, rto:Int, segment:String)

  case class MonthlyResult(dt:String, rto:Int, segment:String, count:Long)

  def makeParquets(spark:SparkSession) = {
    import spark.implicits._
    val path = "in"
    val seq = (for {i <- 1 to 1000000} yield generateRandomRow3G()).toDS()
    seq.write.partitionBy("dt","h").parquet(path)
    path
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Monthly Segmented Process For DataVentures (Stats NZ)")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._
    import AreaToRTOConverter.converter

//    makeParquets(spark)

    // get us some data
    val reads = spark.read.parquet("in").as[Row3G]

    val rows = reads.map(row => new DeviceReadRow(row));

    val withoutFakes = rows
      .groupByKey(row => (row.dt,row.h,row.imei))
      .reduceGroups((x, y) => getEarliestRow(x, y))
      .map(_._2)

    val threeAm = withoutFakes.filter(row => (row.h > 20) || (row.h <= 3))
      .flatMap((v) => makeFakes(v))
      .filter(row => row.h == 3)
      .groupByKey(row => (row.imei, row.dt))
      .reduceGroups((x, y) => getEarliestRow(x, y))
      .map(_._2)
      .groupByKey(row => (row.imei, if (row.international()) "international" else row.area))
      .count()



    val imeiHomes = threeAm.groupByKey(row => row._1._1)
      .reduceGroups((a,b) => biggestCount(a,b))
      .map(x => new ImeiHome(x._2._1._1, x._2._1._2)) // the value, has a kv pair of imei,area as its first part.



    val monthly = rows.map(row => ImeiMonthly(row.imei, converter(row.area), DateTime.parse(row.starttime).monthOfYear().roundFloorCopy().toString("yyyy-MM-dd")))
      .distinct()

    val joined = monthly.as("monthly").joinWith(imeiHomes.as("imei_homes"),$"monthly.imei" === $"imei_homes.imei")

    def toResult(x:(ImeiMonthly, ImeiHome)): ImeiMonthlyResult = {
      val monthly = x._1
      val home = x._2
      val segment = if (home.rto == 0) "international" else if (home.rto == monthly.rto) "local" else "domestic"
      ImeiMonthlyResult(monthly.dt, monthly.rto, segment)
    }

    val results = joined.map(x => toResult(x)).groupByKey(x => x).count().map((x:(ImeiMonthlyResult, Long)) => MonthlyResult(x._1.dt, x._1.rto, x._1.segment, x._2))

    results.coalesce(10).write.csv("joinedOut")

  }

}
