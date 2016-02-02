import breeze.linalg._
import breeze.linalg._
import org.apache.spark.HashPartitioner
val ratings = sc.textFile("hdfs://cshadoop1.utdallas.edu/hw4fall/ratings.dat").map(l => (l.split("::")(0),l.split("::")(1),l.split("::")(2))) 
val numItem = ratings.map(x=>x._2).distinct.count 
val numUser = ratings.map(x=>x._1).distinct.count 
val items = ratings.map(x=>x._2).distinct   
val users = ratings.map(x=>x._1).distinct  
val k= 5  
val matrixItem = items.map(x=> (x,DenseVector.zeros[Double](k)))   
var matrixItem2 = matrixItem.map(x => (x._1,x._2(0 to k-1):=0.5)).partitionBy(new HashPartitioner(10)).persist  
val matrixUser = users.map(x=> (x,DenseVector.zeros[Double](k)))
var matrixUser2 = matrixUser.map(x => (x._1,x._2(0 to k-1):=0.5)).partitionBy(new HashPartitioner(10)).persist 
val ratingByItem = sc.broadcast(ratings.map(x => (x._2,(x._1,x._3)))) 
val ratingByUser = sc.broadcast(ratings.map(x => (x._1,(x._2,x._3)))) 
var i =0
for( i <- 1 to 10){

	val riVector = matrixItem2.join(ratingByItem.value)
	val regfactor = 1.0 
	val regMatrix = DenseMatrix.zeros[Double](k,k)  
	regMatrix(0,::) := DenseVector(regfactor,0,0,0,0).t 
	regMatrix(1,::) := DenseVector(0,regfactor,0,0,0).t 
	regMatrix(2,::) := DenseVector(0,0,regfactor,0,0).t 
	regMatrix(3,::) := DenseVector(0,0,0,regfactor,0).t 
	regMatrix(4,::) := DenseVector(0,0,0,0,regfactor).t
	val userbyItemMat = riVector.map(x => (x._2._2._1,x._2._1*x._2._1.t )).reduceByKey(_+_).map(x=> (x._1,breeze.linalg.pinv(x._2 + regMatrix))) 
	val sumVector = riVector.map(x => (x._2._2._1,x._2._1 * x._2._2._2.toDouble )).reduceByKey(_+_) 
	val resJoin = userbyItemMat.join(sumVector) 
	matrixUser2 = resJoin.map(x=> (x._1,x._2._1 * x._2._2)).partitionBy(new HashPartitioner(10)) 

val vectorRU = matrixUser2.join(ratingByUser.value)
	val itembyUserMat = vectorRU.map(x => (x._2._2._1,x._2._1*x._2._1.t )).reduceByKey(_+_).map(x=> (x._1,breeze.linalg.pinv(x._2 + regMatrix))) 
	val sumVectorxu = vectorRU.map(x => (x._2._2._1,x._2._1 * x._2._2._2.toDouble )).reduceByKey(_+_) 
	val resJoin1 = itembyUserMat.join(sumVectorxu) 
	matrixItem2 = resJoin1.map(x=> (x._1,x._2._1 * x._2._2)).partitionBy(new HashPartitioner(10)) 
}

val user1 = matrixUser2.filter(x=>(x._1.equals("1")))
val item1 = matrixItem2.filter(x=>(x._1.equals("914")))
val arrayUser1 = new DenseVector(user1.values.toArray)
val arrayItem1 = new DenseVector(item1.values.toArray)
val arrayFrat1 = arrayUser1 dot arrayItem1

val user2 = matrixUser2.filter(x=>(x._1.equals("1757")))
val item2 = matrixItem2.filter(x=>(x._1.equals("1777")))
val arrayUser2 = new DenseVector(user2.values.toArray)
val arrayItem2 = new DenseVector(item2.values.toArray)
val arrayFrat2 = arrayUser2 dot arrayItem2

val user3 = matrixUser2.filter(x=>(x._1.equals("1759")))
val item3 = matrixItem2.filter(x=>(x._1.equals("231")))
val arrayUser3 = new DenseVector(user3.values.toArray)
val arrayItem3 = new DenseVector(item3.values.toArray)
val arrayFrat3 = arrayUser3 dot arrayItem3

print(arrayUser1)
print(arrayItem1)
print(arrayFrat1)
print(arrayUser2)
print(arrayItem2)
print(arrayFrat2)
print(arrayUser3)
print(arrayItem3)
print(arrayFrat3)
//1,1757 and 1759.
//914, 1777 and 231.



