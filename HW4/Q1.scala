import breeze.linalg._
val info = sc.textFile("/jkb140130/hw4/itemusermat")
val nameMuv = sc.textFile("/jkb140130/hw4/movies.dat").map(l => (l.split("::")(0),(l.split("::")(1),l.split("::")(2))))
val movies = info.map(l=>(l.split(" ")(0),l.split(" ").drop(1).mkString))

def func1(line: String): Vector[Double] = {
    DenseVector(line.split("").map(_.toDouble))
  }
  
val vectorMuv = movies.mapValues(func1 _)

val ptsFul = vectorMuv.takeSample(false, 10)
val ptsC = ptsFul.map(l =>l._2).toArray

def func2nearPts(p: Vector[Double], centroids: Array[Vector[Double]]): Int = {
    var idxBst = 0
    var nearest = Double.PositiveInfinity

    for (i <- 0 until centroids.length) {
      val dist = squaredDistance(p, centroids(i))
      if (dist < nearest) {
        nearest = dist
        idxBst = i
      }
    }

    idxBst
  }

var dist = 1.0

    for(i <- 0 until 10) {
	
     val nearest = vectorMuv.map (p => (func2nearPts(p._2, ptsC), (p._2, 1,p._1)))
	 val infoClustList = nearest.map(l => (l._1,l._2._3)).groupByKey()
     val clostPt = nearest.map(l => ((l._1),(l._2._1,l._2._2)))
      val infoPoint = clostPt.reduceByKey{case ((p1, c1), (p2, c2)) => (p1 + p2, c1 + c2)}

      val ptsNew = infoPoint.map {pair =>
        (pair._1, pair._2._1 * (1.0 / pair._2._2))}.collectAsMap()

      dist = 0.0
      for (i <- 0 until 10) {
        dist += squaredDistance(ptsC(i), ptsNew(i))
      }

      for (newP <- ptsNew) {
        ptsC(newP._1) = newP._2
      }
	  println("Final Iteration:"+(i+1))
    }
	
	
	for((centre,centreIndex)<-ptsC.zipWithIndex)
	{
		println("Cluster"+centreIndex)
		val infoMuvie = vectorMuv.filter(x=>func2nearPts((x._2),ptsC)== centreIndex ).take(5)
		val infoMuvie2 = sc.parallelize(infoMuvie)
		val finalRes = infoMuvie2.join(nameMuv).foreach(x => println(x._1+" || "+x._2._2._1+" || "+x._2._2._2))
		
	}
