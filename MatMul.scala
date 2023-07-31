import org.apache.spark.SparkContext
//imported libraries to experiment with code
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object Multiply {
  def main(args: Array[ String ]) {
	val cbandif = new SparkConf().setAppName("Gguna Vicched")
 /**val spark = SparkSession.builder()
      .appName("Gguna Viched")
      .getOrCreate()*/
	val sbandac = new SparkContext(cbandif)
//changed names to more familiarised versions

	val mmatrix_pehli = sbandac.textFile(args(0)).map(line => { 
							val readLine = line.split(",")
							(readLine(0).toInt, readLine(1).toInt, readLine(2).toDouble)
						} )	

//The second matrix is used here and should be in given format

	val nmatrix_dusri = sbandac.textFile(args(1)).map(line => { 
							val readLine = line.split(",")
							(readLine(0).toInt, readLine(1).toInt, readLine(2).toDouble)
						} )

	val multiply = mmatrix_pehli.map( mmatrix_pehli => (mmatrix_pehli._2, mmatrix_pehli)).join(nmatrix_dusri.map( nmatrix_dusri => (nmatrix_dusri._1,nmatrix_dusri)))
									 .map{ case (zee, (mmatrix_pehli,nmatrix_dusri)) => 
										((mmatrix_pehli._1,nmatrix_dusri._2),(mmatrix_pehli._3 * nmatrix_dusri._3)) }

	val badlaMolya = multiply.reduceByKey((x,y) => (x+y))

//reduce the values and using key value pairs to multiply

	val kram = badlaMolya.sortByKey(true, 0)
//ordered the pairs in a sorted manner
	val uttar = kram.collect()

//displaying the result	
	uttar.foreach(println)

	sbandac.stop()
//stop allocaion of resources	
  }
}