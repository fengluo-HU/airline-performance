# Notes on Hadoop
I used this website to set up stand-alone hadoop: 
`https://hadoop.apache.org/docs/r3.2.0/hadoop-project-dist/hadoop-common/SingleCluster.html`

After setting up and startup the local dfs, visit this url:
`http://localhost:45003` and this url for namenode: 
`localhost:9870` and this url for yarn: 
`http://localhost:8088` 

*** Note: If you run in to 'lock' problem, may want to delete temporary file in /tmp folder

I used the following information to create user for mapreduce WordCount:

## Start Hadoop

```
hdfs namenode –format
cd /usr/local/hadoop/sbin
./start-all.sh

jpsOutput:
XXXX SecondaryNameNode
XXXX DataNode
XXXX NodeManager
XXXX NameNode
XXXX ResourceManager
XXXX Jps
```

## Word Count program

### Preparation directory structure:
```
hdfs dfs -mkdir /user/
hdfs dfs -mkdir /user/student
hdfs dfs -mkdir /user/student/airline
hdfs dfs -mkdir /user/student/airline/output
hdfs dfs -mkdir /user/student/airline/output/1987
hdfs dfs -mkdir /user/student/airline/output/2008
```

Trouble with the run.

### updated mapred-site.xml
```
<configuration>
        <property>
                <name>mapreduce.framework.name</name>
                <value>yarn</value>
        </property>
        <property>
                <name>yarn.app.mapreduce.am.env</name>
                <value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value>
                </property>
        <property>
                <name>mapreduce.map.env</name>
                <value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value>
        </property>
        <property>
                <name>mapreduce.reduce.env</name>
                <value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value>
        </property>

</configuration>

```

### Running Classic WordCount program

Program is located here: `https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html`

Text: Romeo and Juliet located here: `http://shakespeare.mit.edu/romeo_juliet/full.html`

### Clone & Build:cd ~/dev
```bash
git clone https://github.com/drkiettran/mapreduce
cd mapreduce
mvn clean package
```

### Prepare Input & Run:
```
hdfs dfs -copyFromLocal ~/dev/airline/1987.csv /user/student/airline
hdfs dfs -copyFromLocal ~/dev/airline/2008.csv /user/student/airline

hadoop jar target/cisc-525-mapreduce-jar-with-dependencies.jar
com.drkiettran.mapreduce.FlightsOriginatedFromAirpoirt
/user/student/airline/1987.csv
/user/student/airline/output/1987/flights_by_originate
```

### Clear output:
```
hadoop dfs -rm output/*
hadoop dfs -rmdir output
```

### Result:
```
hdfs dfs -cat /user/student/airline/output/1987/flights_by_originate/part-r-00000 
hdfs dfs -cat /user/student/airline/output/2008/flights_by_originate/part-r-00000 

```

## Hive Getting started:

https://cwiki.apache.org/confluence/display/Hive/GettingStarted

## Mysql JDBC Driver download

- Go here: https://dev.mysql.com/downloads/connector/j/
- Choose Looking for previous GA version (i.e., MYSQL 5.1.47)
- Choose platform independent
- Download Zip.
- Unpack the zip
- copy the mysql-connector-java-5.1.47.jar into ~/hive/lib folder.

## hive/lib
Need to have mysql-connector-java-5.1.47.jar stored in ~/hive/lib folder

### hive/config/hive-site.xml

``` xml

<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>

	<property>
  		<name>javax.jdo.option.ConnectionURL</name>
  		<value>jdbc:mysql://localhost/metastore?createDatabaseIfNotExist=true</value>
	</property>


	<property>
  		<name>javax.jdo.option.ConnectionDriverName</name>
  		<value>com.mysql.jdbc.Driver</value>
	</property>

	<property>
  		<name>javax.jdo.option.ConnectionUserName</name>
  		<value>root</value>
	</property>

	<property>
  		<name>javax.jdo.option.ConnectionPassword</name>
  		<value>password</value>
	</property>

	<property>
  		<name>datanucleus.autoCreateSchema</name>
  		<value>true</value>
	</property>

	<property>
  		<name>datanucleus.fixedDatastore</name>
  		<value>true</value>
	</property>

	<property>
 		<name>datanucleus.autoCreateTables</name>
 		<value>True</value>
 	</property>

</configuration>
```
## Sample code for 'phrase count'

```java
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private static final Logger LOGGER = LoggerFactory.getLogger(TokenizerMapper.class);

		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			Text word = new Text();
			Text lastWord = new Text("");
			Text twoWords = new Text();

			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				twoWords.set(lastWord.copyBytes());
				twoWords.append(" ".getBytes(), 0, 1);
				twoWords.append(word.copyBytes(), 0, word.getLength());
				context.write(twoWords, one);
				lastWord.set(word.copyBytes());
			}
		}
	}

```