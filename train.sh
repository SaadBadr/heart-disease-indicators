export HADOOP_CLASSPATH=$(hadoop classpath)
javac -classpath $HADOOP_CLASSPATH -d mean_classes NB.java
javac -classpath $HADOOP_CLASSPATH -d var_classes NBVar.java
jar -cvf NB.jar -C mean_classes .
jar -cvf NBVar.jar -C var_classes .

hadoop fs -rm -r /NBTut/Input
hadoop fs -put Input/training_data.csv /NBTut/Input
hadoop fs -rm -r /NBTut/Output

hadoop jar NB.jar NB /NBTut/Input /NBTut/Output

hadoop dfs -cat /NBTut/Output/* > mean.txt
hadoop fs -rm -r /NBTut/Output2/params.txt
hadoop fs -put mean.txt /NBTut/Output2/params.txt

hadoop fs -rm -r /NBTut/Output

hadoop jar NBVar.jar NBVar /NBTut/Input /NBTut/Output
hadoop dfs -cat /NBTut/Output/* > variance.txt

cat mean.txt variance.txt > model.txt