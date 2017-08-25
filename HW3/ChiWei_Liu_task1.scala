/**
  * Created by Brian on 2017/3/20.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import java.io._

object modelBasedCF {
  def main(args: Array[String]) {
    val pw = new PrintWriter(new File("ChiWeiLiu Scala.txt"))
    pw.println("UserId,MovieId,Pred_rating")
    val conf = new SparkConf().setAppName("ChiWeiLiu_hw3-task1").setMaster("local")
    val sc = new SparkContext(conf)
    val  testingPath = args(0)
    val  trainingPath = args(1)
    val testingRdd = sc.textFile(testingPath)
    val trainingRdd = sc.textFile(trainingPath)

    val headerFirst = testingRdd.first()
    val headerSec = trainingRdd.first()
    val testRddFirst=testingRdd.filter{ x => x!=headerFirst}.map(line => line.split(",")).map{x => ((x(0),x(1)),1)}
    //    testRdd1.foreach(println)
    val trainingRdd1=trainingRdd.filter{ x => x!=headerSec}.map(line => line.split(",")).map{x => ((x(0),x(1)),x(2))}

    val data = trainingRdd1.subtractByKey(testRddFirst).map{ case ((user, product), rate) => (user, product, rate)}

    val ratings = data.map{ case (user, item, rate) => Rating(user.toInt, item.toInt, rate.toDouble)}

    val rank = 10
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.1)

    val usersProducts = testRddFirst.map{ case ((user, product), 1) => (user.toInt, product.toInt)}
    val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) => (user, product, rate)}

    val predictionFirst = predictions.map { case (user, product, rate) =>((user.toInt, product.toInt), rate.toDouble)}


    val testRddSec = testRddFirst.map{  case ((user, product), rate) => ((user.toInt, product.toInt), rate.toDouble)}
    val  avg = data.map{ case (user, product, rate) => (user.toInt, rate.toDouble)}.groupByKey().map{ x=>(x._1, x._2.reduce(_+_)/x._2.count(x=>true))}
    val whole = testRddSec.subtractByKey(predictionFirst).map{ case ((user, product), rate)=>(user, product)}.join(avg).map { case (user, (product, rate)) =>(user, product, rate)}

    val predictionSec = whole.map { case (user, product, rate) =>((user.toInt, product.toInt), rate.toDouble)}
    val res=predictions.union(whole).sortBy{case (a,b,c)=>(a,b)}


    for (item <- res.collect)
    { val str=item.toString()
      val nstr=str.substring(1,str.length-1)
      pw.println(nstr)}

    val ratesAndPredFirst = trainingRdd1.map { case ((user, product), rate) => ((user.toInt, product.toInt), rate.toDouble)}.join(predictionFirst)
    val ratesAndPredSec =trainingRdd1.map { case ((user, product), rate) => ((user.toInt, product.toInt), rate.toDouble)}.join(predictionSec)
    val ratesAndPred=ratesAndPredFirst.union(ratesAndPredSec)
    val MSE = ratesAndPred.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err }.mean()
    val  RMSE=math.sqrt(MSE)
    val diff= ratesAndPred.map { case ((user, product), (r1, r2)) => math.abs(r1 - r2)}

    var num1=0
    var num2=0
    var num3=0
    var num4=0
    var num5=0

    for (item <- diff.collect) {
      item match {
        case item if (item>=0 && item<1) => num1 = num1 + 1;
        case item if (item>=1 && item<2) => num2 = num2 + 1;
        case item if (item>=2 && item<3) => num3 = num3 + 1;
        case item if (item>=3 && item<4) => num4 = num4 + 1;
        case item if (item>=4 ) => num5 = num5 + 1;
      }
    }

    println(">=0 and <1:"+ num1)
    println(">=1 and <2:"+ num2)
    println(">=2 and <3:"+ num3)
    println(">=3 and <4:"+ num4)
    println(">=4 :"+ num5)
    println("RMSE = " + RMSE)
    pw.close()
  }
}
