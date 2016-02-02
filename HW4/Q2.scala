import org.apache.spark.util.Vector
import breeze.linalg._
import math.sqrt
val info = sc.textFile("/jkb140130/hw4/itemusermat")
val nameMuv = sc.textFile("/jkb140130/hw4/movies.dat").map(l => (l.split("::")(0),(l.split("::")(1),l.split("::")(2))))
val movies = info.map(l=>(l.split(" ")(0), Vector(l.split(" ").drop(1).map(_.toDouble))))
println("Enter the movie id:")
	val input = readLine()
	println(input)

def dbAvg(vec: Vector[Double]): Double ={
    var count = 0.0
	var sum = 0.0
	for (i <- 0 until vec.length){
		if(vec(i) != 0.0 || vec(i) != 0 ){
		   sum = sum+vec(i)
		   count = count + 1
		}
	}
	var avg = sum / count
	return avg
}
val dbAvg = movies.map(x=> (x._1,dbAvg(x._2)))
val coefficient = movies.join(dbAvg)
val pear_coefficient = coefficient.map(x => (x._1,(x._2._1:-=x._2._2)))

val pcMuvi = pear_coefficient.filter(x => x._1.contains(input))

def cosine_sim(x:Vector[Double],y:Vector[Double]):Double={
  val dotProd = x dot y
  val vectorDump =DenseVector.zeros[Double](x.length)
  val denomtr = (sqrt(squaredDistance(x,vectorDump)))* (sqrt(squaredDistance(y,vectorDump)))
 
  val cosineSimilarity = dotProd/denomtr
  return cosineSimilarity
}


val crossprod = pcMuvi.cartesian(pear_coefficient)
val cos_sim = crossprod.map{case ((k1, v1), (k2, v2)) => ((k1,k2),(cosine_sim(v1,v2)))}

val similarityMuv1 = cos_sim.sortBy(-_._2).take(6)
val similarityMuv2 = sc.parallelize(similarityMuv1)
val similarityMuvi = similarityMuv2.map(x => ((x._1._2),(x._2)))

val finalRes = similarityMuvi.join(nameMuv).foreach(x => println(x._1+" || "+x._2._2._1+" || "+x._2._2._2+"  || "+x._2._1))






