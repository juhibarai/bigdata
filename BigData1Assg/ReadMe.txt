
Name : Juhi Kaushik Barai


Create 3 input folders: input, input1 and input2. Put business.csv in input, review.csv in input1 and user.csv in input2 directory.

Make input directory:
hdfs dfs -mkdir input

Put business.csv in input directory:
hdfs dfs -put business.csv input

Make input directory:
hdfs dfs -mkdir input1   

Put review.csv in input directory:
hdfs dfs -put review.csv input1

Make input directory:
hdfs dfs -mkdir input2   

Put user.csv in input directory:
hdfs dfs -put user.csv input2


Question 1

Run the jar file:
hadoop jar BDAssg1.jar bigdata.assignment1.Q1 /jkb140130/input /jkb140130/firstop

Check the output:
hdfs dfs -cat /jkb140130/firstop/*


Question 2

Run the jar file:
hadoop jar BDAssg2.jar bigdata.assignment1.Q2 /jkb140130/input1 /jkb140130/secondop

Check the output:
hdfs dfs -cat /jkb140130/secondop/*


Question 3

Run the jar file:
hadoop jar BDAssg3.jar bigdata.assignment1.Q3 /jkb140130/input1 /jkb140130/thirdop


Question 4

Run the jar file:
hadoop jar BDAssg4.jar bigdata.assignment1.Q4 /jkb140130/input /jkb140130/fourthop

Check the output:
hdfs dfs -cat /jkb140130/fourthop/part-r-00000
