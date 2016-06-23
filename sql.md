### big-data-competition_dxc
================================
# 考虑使用线性回归做
### (根据前两天的，预测第三天的播放记录。然后找到每个歌手8.29和8.30的记录，预测出8.31的记录，然后以预测出的8.31的记录为基础继续往下预测)
##### 解析文本(获得一些基本信息)
    //定义样例类
    case class SongInfo(songid: String, artistid: String, publishTime: String, initialPlayTimes: Int, language: Int, gender: Int)  
    //将格式化文本转换为RDD，其中文本的每一行就是RDD中的一项纪录
    val songText = sc.textFile("/opt/meizhang/trycache/mars_tianchi_songs.csv")
    //RDD的transformation，将RDD中存储的string类型转换为定义的样例类SongInfo
    val songinfo = songText.map( s => s.split(",")).filter(x =>x.length==6).map(s=>SongInfo(s(0), s(1), s(2), s(3).toInt, s(4).toInt, s(5).toInt)) 
    //将RDD转换为dataframe
    val song = songinfo.toDF
    song.show()
    //将DF注册成一个名为“songinfo”的临时表，可以使用sql语句在表中查询， eg： select * from songinfo
    val res = song.registerTempTable("songinfo")
    //同理，将useraction的信息读进来，也注册为表
    case class UsersAction(userid: String, songid: String, playTime: String, actionType: Int, date: String)
    val usersActionText = sc.textFile("/opt/meizhang/trycache/test_mars_tianchi_user_actions.csv")
    val useractionInfo = usersActionText.map(s => s.split(",")).filter(x => x.length==5).map(s=> UsersAction(s(0), s(1),s(2),s(3).toInt,s(4)) )
    val rawUserAction = useractionInfo.toDF.registerTempTable("rawUserAction")  //包含重复数据
    val useraction = sqlContext.sql("select distinct *  from rawUserAction").registerTempTable("useraction") //去除重复数据

##### 将两张表拼接成为一张大表，sqlContext.sql返回DF
    val allInfo = sqlContext.sql("select date, userid, s.songid, actionType, playTime, artistid, publishtime, initialPlayTimes, language, gender from useraction as u left join songinfo as s on u.songid=s.songid") 

##### 为了将表示今天的字符串，转换成表示明天的字符串
    import java.text.ParsePosition
    import java.text.SimpleDateFormat
    import java.util.Calendar
    import java.util.Date
    import java.util.GregorianCalendar
    object Convert extends Serializable{
        def findYesterday( date: String) : String = {
          val sdf : SimpleDateFormat =new SimpleDateFormat("yyyyMMdd")
          val st = sdf.parse(date, new ParsePosition(0))
          val cal : GregorianCalendar  = new GregorianCalendar();
          cal.setTime(st);
          cal.add(Calendar.DAY_OF_MONTH, +1);
          val result = sdf.format(cal.getTime());
          return result;
        } 

##### 统计播放量 下载量  收藏量（同时，记录昨天的数据，前天的数据）
    //需要重新定义一个case class,用来分别统计歌曲的播放量，下载量和收藏量，也就是将actionType分开统计
    case class RealInfo(date: String, songid: String, broadcast: Int, download: Int, collect: Int)
    import org.apache.spark.sql._
    val today = allInfo.groupBy("date", "songid", "actionType").count  //dataframe
    //首先将today转换为RDD
    val todayRDD = today.map(x => (x.getString(0), x.getString(1), x.getInt(2), x.getLong(3).toInt))
    //有点难理解，（3.14 23 15 1） （3.15 25 16 3）=》（3.15的昨天 23 15 1）这一天的昨天 是这么多记录 
    val yesterdayRDD = todayRDD.map(x => ( Convert.findYesterday(x._1), x._2, x._3, x._4))
    val agoRDD = yesterdayRDD.map(x => ( Convert.findYesterday(x._1), x._2, x._3, x._4))  
    //有点难理解，每行数据，变成了这种（3.15 离歌 3 0 0 0）（3.15 离歌 0 4 0 ）（3.15 离歌 0 0 5） 之后需要把这些合并在一起，变成（3.15 离歌 3 4 5）
    val yesterdayDF =  yesterdayRDD.map(x => RealInfo(x._1, x._2, if(x._3==1)x._4 else 0, if(x._3==2) x._4 else 0,if(x._3==3) x._4 else 0)).toDF
    val agoDF = agoRDD.map(x => RealInfo(x._1, x._2, if(x._3==1)x._4 else 0, if(x._3==2) x._4 else 0,if(x._3==3) x._4 else 0)).toDF
    val todayDF = todayRDD.map(x => RealInfo(x._1, x._2, if(x._3==1)x._4 else 0, if(x._3==2) x._4 else 0,if(x._3==3) x._4 else 0)).toDF
    todayDF.registerTempTable("todayDF")
    yesterdayDF.registerTempTable("yesterdayDF")
    agoDF.registerTempTable("agoDF")

##### //合并 （3.15 离歌 3 0 0 0）（3.15 离歌 0 4 0 ）（3.15 离歌 0 0 5） 之后需要把这些合并在一起，变成（3.15 离歌 3 4 5)
    val todayCollect = sqlContext.sql("select date,songid, sum(broadcast) as playTime,sum(download) as download,sum(collect) as collect 
    from todayDF group by date,songid order by date")
    val yesterdayCollect = sqlContext.sql("select date,songid, sum(broadcast) as playTime,sum(download) as download,sum(collect) as collect from yesterdayDF group by date,songid order by date")
    val agoCollect = sqlContext.sql("select date,songid, sum(broadcast) as playTime,sum(download) as download,sum(collect) as collect from agoDF group by date,songid order by date")
    todayCollect.registerTempTable("todayCollect")
    yesterdayCollect.registerTempTable("yesterdayCollect")
    agoCollect.registerTempTable("agoCollect")

##### //将今天，昨天，后天的数据统计到一个表里
    //今天的表跟昨天的表join，生成join1  dataFrame
    val join1 = todayCollect.join(yesterdayCollect, Seq("date", "songid"), "left_outer").select(todayCollect("*"), yesterdayCollect("playTime").as("yesterdayPlay"), yesterdayCollect("download").as("yesterdayDown"), yesterdayCollect("collect").as("yesterdayC")) 
    //前两天的大表与前三天的表左连接
    val joinFinally = join1.join(agoCollect,Seq("date", "songid"), "left_outer").select(join1("*"), agoCollect("playTime").as("agoPlay"), agoCollect("download").as("agoDown"), agoCollect("collect").as("agoC")).registerTempTable("joinFinally")

##### //将统计出的信息与包含歌曲全部信息的allinfo表拼接，将播放量，下载量，收藏量作为标签，即（要预测的量）
    val allSongInfo = sqlContext.sql("select jf.date, jf.songid, al.playTime, download, collect, yesterdayPlay, yesterdayDown, yesterdayC, agoPlay, agoDown, agoC, artistid, publishtime, initialPlayTimes, language, gender from joinFinally as jf, allInfo as al where jf.songid=al.songid and jf.date=al.date order by jf.date").registerTempTable("allSongInfo")
    val finalResult = sqlContext.sql("select * from allSongInfo").rdd.repartition(1).saveAsTextFile("/opt/meizhang/trycache/result_test_final") //这个dataframe包含某天某首歌今天，昨天，前天的播放量，下载量，收藏量, 

#进一步处理(做标准化和PCA)
----------
##### 读数据，注册成表
	//定义样例类
	case class SongInfo(date: String, songId: String, play: Int, download: Int, collect: Int, yesterdayPlay: Int, yesterdayDown: Int, yesterdayCollect: Int, playRate: Int, downloadRate: Int, collectRate: Int, time: Int, artistid: String, publishtime: Int, initialPlayTimes: Int, language: Int, gender: Int)  
	//将格式化文本转换为RDD，其中文本的每一行就是RDD中的一项纪录
	val songText = sc.textFile("/opt/meizhang/trycache/refine_growth_result/part-00000")
	//RDD的transformation，将RDD中存储的string类型转换为定义的样例类SongInfo
	val songinfo = songText.map( s => s.substring(1, s.length - 1).split(",")).filter(x =>x.length==17).map(s=>SongInfo(s(0), s(1), s(2).toInt, s(3).toInt, s(4).toInt, s(5).toInt, s(6).toInt, s(7).toInt, s(8).toInt, s(9).toInt, s(10).toInt, s(11).toInt, s(12), s(13).toInt, s(14).toInt, s(15).toInt, s(16).toInt)) 
	//将RDD转换为dataframe
	val song = songinfo.toDF
	song.registerTempTable("songInfo")

##### 把morning和afternoon的信息分开
	def getTimes(t: Int) = if (t < 8) (1, 0) else (0 , 1)
	
	case class SongInfo_1(date: String, songId: String, play: Int, download: Int, collect: Int, yesterdayPlay: Int, yesterdayDown: Int, yesterdayCollect: Int, playRate: Int, downloadRate: Int, collectRate: Int, morning: Int, afternoon: Int, artistid: String, publishtime: Int, initialPlayTimes: Int, language: Int, gender: Int)
	
	val songInfo_1 = song.map(s => SongInfo_1(s.getString(0), s.getString(1), s.getInt(2), s.getInt(3), s.getInt(4), s.getInt(5), s.getInt(6), s.getInt(7), s.getInt(8), s.getInt(9), s.getInt(10), getTimes(s.getInt(11))._1, getTimes(s.getInt(11))._2, s.getString(12), s.getInt(13), s.getInt(14), s.getInt(15), s.getInt(16))).toDF
	songInfo_1.registerTempTable("songInfo_1")
	songInfo_1.show()

##### 把morning和afternoon的信息汇总起来
	val songInfo_2 = sqlContext.sql("select date, songId, play, download, collect, yesterdayPlay, yesterdayDown, yesterdayCollect, playRate, downloadRate, collectRate, sum(morning) as morningTimes, sum(afternoon) as afternoonTimes, artistid, publishtime, initialPlayTimes, language, gender from songInfo_1 group by date,songId,play,download,collect,yesterdayPlay,yesterdayDown,yesterdayCollect,playRate,downloadRate,collectRate,artistid,publishtime,initialPlayTimes,language,gender order by date, songId")
	songInfo_2.registerTempTable("songInfo_2")
	songInfo_2.show()

##### <font color="red">保存标准化之前的数据</font>
	val result_stand_before = sqlContext.sql("select * from songInfo_2").repartition(1).write.parquet("/opt/xcdong/trycache/result_stand_before") //标准化之前的数据

##### 读数据并注册成表
	val result = sqlContext.read.parquet("/opt/xcdong/trycache/result_stand_before/part-r-00000-3024dba2-f98e-49d2-b533-6f721c5f589a.gz.parquet")
	result.first()
    result.registerTempTable("stand_before")

##### 计算特征的均值和方差
	import org.apache.spark.sql.Row
	import org.apache.spark.mllib.linalg.Vectors  
	import java.lang.Double
	import java.lang.Long
	import org.apache.spark.mllib.stat.Statistics
	
	val data = result.map(f => Vectors.dense(f.getInt(5).toDouble, f.getInt(6).toDouble, f.getInt(7).toDouble, f.getInt(8).toDouble, f.getInt(9).toDouble, f.getInt(10).toDouble, f.getLong(11).toDouble, f.getLong(12).toDouble, f.getInt(14).toDouble, f.getInt(15).toDouble, f.getInt(16).toDouble, f.getInt(17).toDouble))
	val stat = Statistics.colStats(data)
	val mean = stat.mean
	val variance = stat.variance

##### 标准化特征并注册成表（准备对时间排序）
	def normalize(x: Int, y: Double, z:Double) = {(x - y) / z}
	
	case class Stand_after(date: String, songId: String, play: Int, download: Int, collect: Int, yesterdayPlay: Double, yesterdayDown: Double, yesterdayCollect: Double, playRate: Double, downloadRate: Double, collectRate: Double, morning: Double, afternoon: Double, artistid: String, publishtime: Double, initialPlayTimes: Double, language: Double, gender: Double)
	
	result.map(f => Stand_after(f.getString(0), f.getString(1), f.getInt(2), f.getInt(3), f.getInt(4), normalize(f.getInt(5), mean(0), variance(0)), normalize(f.getInt(6), mean(1), variance(1)), normalize(f.getInt(7), mean(2), variance(2)), normalize(f.getInt(8), mean(3), variance(3)), normalize(f.getInt(9), mean(4), variance(4)),normalize(f.getInt(10), mean(5), variance(5)), normalize(f.getLong(11).toInt, mean(6), variance(6)), normalize(f.getLong(12).toInt, mean(7), variance(7)), f.getString(13), normalize(f.getInt(14), mean(8), variance(8)), normalize(f.getInt(15), mean(9), variance(9)), normalize(f.getInt(16), mean(10), variance(10)), normalize(f.getInt(17), mean(11), variance(11)))).toDF.registerTempTable("stand_after_1")

##### 对时间排序后<font color="red">保存为标准化之后的数据</font>
	sqlContext.sql("select * from stand_after_1 order by date").repartition(1).write.parquet("/opt/xcdong/trycache/result_stand_after")

##### 读数据,注册成表，方处理
	val stand_after = sqlContext.read.parquet("/opt/xcdong/trycache/result_stand_after/part-r-00000-e67820e0-02f0-4196-ab7f-0054591461aa.gz.parquet")
	stand_after.registerTempTable("stand_after")
	stand_after.first()
##### 对特征进行PCA处理
	import org.apache.spark.mllib.linalg.{Vector, Vectors}
	import org.apache.spark.mllib.regression.LabeledPoint
	import org.apache.spark.mllib.feature.PCA
	
	case class Prop(data: String, songId: String, artistid: String)
	case class Labels(play: Int, download: Int, collect: Int)
	case class LabelsPoint(prop: Prop, labels: Labels, features: Vector)
	
    //为了保存标签值
	val labelsPoint = stand_after.map(s => LabelsPoint(Prop(s.getString(0), s.getString(1), s.getString(13)), Labels(s.getInt(2), s.getInt(3), s.getInt(4)),Vectors.dense(s.getDouble(5), s.getDouble(6), s.getDouble(7), s.getDouble(8),s.getDouble(9), s.getDouble(10), s.getDouble(11), s.getDouble(12), s.getDouble(14), s.getDouble(15), s.getDouble(16), s.getDouble(17))))
	labelsPoint.first().features().size()
	//训练PCA模型
	val pca =new PCA(8).fit(labelsPoint.map(_.features))
	//用模型，对样本进行PCA转换
	val pca_after= labelsPoint.map(p => p.copy(features = pca.transform(p.features)))
	
	pca_after.first().features().size()
##### <font color="red">保存为PCA之后的数据</font>
	pca_after.toDF.repartition(1).write.parquet("/opt/xcdong/trycache/pca_after")

### 训练模型
	import org.apache.spark.mllib.regression.LabeledPoint
	import org.apache.spark.mllib.regression.LinearRegressionModel
	import org.apache.spark.mllib.regression.LinearRegressionWithSGD
	import org.apache.spark.mllib.linalg.Vectors
	
	val result = sqlContext.read.parquet("/opt/xcdong/trycache/pca_after/part-r-00000-2d51c0cb-de66-49fa-ba5e-9dec132ddb74.gz.parquet") 
	result.first()
##### 生成标签向量	
	val parsedPlayData = result.map(s =>LabeledPoint(s.getStruct(1).getInt(0), s.getAs[Vector]("features")))
	val parsedDownloadData = result.map(s =>LabeledPoint(s.getStruct(1).getInt(1), s.getAs[Vector]("features")))
	val parsedCollectData = result.map(s =>LabeledPoint(s.getStruct(1).getInt(2), s.getAs[Vector]("features")))

##### 训练模型
	val numIterations = 10000
	val stepSize = 0.000000001
	val playModel = LinearRegressionWithSGD.train(parsedPlayData, numIterations, stepSize)
	val downloadModel = LinearRegressionWithSGD.train(parsedDownloadData, numIterations, stepSize)
	val collectModel = LinearRegressionWithSGD.train(parsedCollectData, numIterations, stepSize)
##### 在测试数据集上判断，模型训练的好坏
	val valuesAndPredsPlay = parsedPlayData.map { point =>
		val prediction = playModel.predict(point.features)
		(point.label, prediction)
	}
	val valuesAndPredsDownload = parsedDownloadData.map { point =>
		val prediction = downloadModel.predict(point.features)
		(point.label, prediction)
	}
	val valuesAndPredsCollect = parsedCollectData.map { point =>
		val prediction = collectModel.predict(point.features)
		(point.label, prediction)
	}
	valuesAndPredsPlay.collect()
	valuesAndPredsDownload.collect()

# 但是，最终结果不是一般的差
训练出来线性模型的权值如下
![](https://github.com/wlwgcdxc/picture/blob/master/weight.PNG)
预测值和标签的对比
![](https://github.com/wlwgcdxc/picture/blob/master/predict.PNG)
从图中可以看到，标签应该不是成线性变化的
![](https://github.com/wlwgcdxc/picture/blob/master/proof1.PNG)
![](https://github.com/wlwgcdxc/picture/blob/master/proof2.PNG)

# 准备使用随机森林，试试非线性的回归或者分类去做
###从歌曲信息中获取歌手信息
	import org.apache.spark.mllib.regression.LabeledPoint
	import org.apache.spark.mllib.regression.LinearRegressionModel
	import org.apache.spark.mllib.regression.LinearRegressionWithSGD
	import org.apache.spark.mllib.linalg.Vectors
	
	//val result = sqlContext.read.parquet("/opt/xcdong/trycache/result_stand_before/part-r-00000-3024dba2-f98e-49d2-b533-6f721c5f589a.gz.parquet") 
	val result = sc.textFile("/opt/xcdong/trycache/textfile/result_stand_before/part-00000") 
	result.first()
	result.cache
	
	case class SongInfo(date: String, song: String, todayPlay: Int, todayDown: Int, todayCollect: Int, yesPlay: Int, yesDown: Int, yesCollect: Int, playRate: Int, downRate: Int, collectRate: Int, morning: Int, afternoon: Int, artist: String, publishTime: Int, initialTimes: Int, language: Int, gender: Int) 
	result.map{ e =>
	  val ele = e.substring(1, e.length - 1).split(",")
	  SongInfo(ele(0), ele(1), ele(2).toInt, ele(3).toInt, ele(4).toInt, ele(5).toInt, ele(6).toInt, ele(7).toInt, ele(8).toInt, ele(9).toInt, ele(10).toInt, ele(11).toInt, ele(12).toInt, ele(13), ele(14).toInt, ele(15).toInt, ele(16).toInt, ele(17).toInt)
	}.toDF.registerTempTable("songInfo")
        
        val artistInfo = sqlContext.sql("select date, artist,  gender, sum(todayPlay) as todayPlay, sum(todayDown) as todayDown, sum(todayCollect) as todayCollect, sum(yesPlay) as yesPlay, sum(yesDown) as yesDown, sum(yesCollect) as yesCollect, sum(playRate) as playRate, sum(downRate) as downRate, sum(collectRate) as collectRate, sum(morning) as morning, sum(afternoon) as afternoon, sum(publishTime) as publishTime, sum(initialTimes) as initialTimes  from songInfo group by date, artist, gender order by date")
	artistInfo.registerTempTable("artistInfo")
	
	artistInfo.rdd.repartition(1).saveAsTextFile("/opt/xcdong/trycache/artistInfo")
	
	//应该把今天早晨和晚上歌曲的播放次数，以及昨天早晨和晚上歌曲的播放次数都放在一起，前者作为标签，后者作为特征
	val result = sc.textFile("/opt/xcdong/trycache/artistInfo/part-00000") 
	result.first()
	result.cache
	
	case class SongInfo(date: String, artist: String, gender: Int, todayPlay: Int, todayDown: Int, todayCollect: Int, yesPlay: Int, yesDown: Int, yesCollect: Int, playRate: Int, downRate: Int, collectRate: Int, morning: Int, afternoon: Int, publishTimes: Int, initialTimes: Int) 
	val today = result.map{ e =>
	  val ele = e.substring(1, e.length - 1).split(",")
	  SongInfo(ele(0), ele(1), ele(2).toInt, ele(3).toInt, ele(4).toInt, ele(5).toInt, ele(6).toInt, ele(7).toInt, ele(8).toInt, ele(9).toInt, ele(10).toInt, ele(11).toInt, ele(12).toInt, ele(13).toInt, ele(14).toInt, ele(15).toInt)
	}.toDF
	today.registerTempTable("todaySongInfo")
	
	import java.text.ParsePosition
	import java.text.SimpleDateFormat
	import java.util.Calendar
	import java.util.Date
	import java.util.GregorianCalendar
	object Convert extends Serializable{
	    def findYesterday( date: String) : String = {
	      val sdf : SimpleDateFormat =new SimpleDateFormat("yyyyMMdd")
	      val st = sdf.parse(date, new ParsePosition(0))
	      val cal : GregorianCalendar  = new GregorianCalendar();
	      cal.setTime(st);
	      cal.add(Calendar.DAY_OF_MONTH, +1);
	      val result = sdf.format(cal.getTime());
	      return result;
	    } 
	}
	
	val yesterday = today.map { e =>
	  SongInfo(Convert.findYesterday(e.getString(0)), e.getString(1), e.getInt(2), e.getInt(3), e.getInt(4), e.getInt(5), e.getInt(6), e.getInt(7), e.getInt(8), e.getInt(9), e.getInt(10), e.getInt(11), e.getInt(12), e.getInt(13), e.getInt(14), e.getInt(15))
	}.toDF
	yesterday.registerTempTable("yesterdaySongInfo")
	
	val join1 = today.join(yesterday, Seq("date", "artist"), "left_outer").select(today("date").as("date"), today("artist").as("artist"), today("gender").as("gender"), today("todayPlay").as("todayPlay"), today("todayDown").as("todayDown"), today("todayCollect").as("todayCollect"), today("morning").as("todayMorning"), today("afternoon").as("todayAfternoon"), today("yesPlay").as("yesPlay"), today("yesDown").as("yesDown"), today("yesCollect").as("yesCollect"), today("playRate").as("playRate"), today("downRate").as("downRate"), today("collectRate").as("collectRate"), yesterday("morning").as("yesMorning"), yesterday("afternoon").as("yesAfternoon"), today("publishTimes").as("publishTimes"), today("initialTimes").as("initialTimes"))
	
	join1.rdd.repartition(1).saveAsTextFile("/opt/xcdong/trycache/artistInfo_right")
	
	//去掉因为表连接产生的null值
	val songText = sc.textFile("/opt/xcdong/trycache/artistInfo_right")
	val songinfo = songText.map( s => s.replace("null", "0"))
	val song = songinfo.repartition(1).saveAsTextFile("/opt/xcdong/trycache/artistInfo_right_1")
###读取歌手信息
	val result = sc.textFile("/opt/xcdong/trycache/artistInfo_right_1/part-00000") 
	result.first()
	result.cache
	
###准备训练数据
	val parsedPlayData = result.map{s1 => 
	                            val s = s1.substring(1, s1.length-1).split(",")
	                            LabeledPoint(s(3).toDouble, Vectors.dense(s(8).toDouble, s(9).toDouble, s(10).toDouble, s(11).toDouble, s(12).toDouble, s(13).toDouble, s(14).toDouble, s(15).toDouble, s(16).toDouble, s(17).toDouble))}.cache
	                            
	val parsedDownData = result.map{s1 => 
	                            val s = s1.substring(1, s1.length-1).split(",")
	                            LabeledPoint(s(4).toDouble, Vectors.dense(s(8).toDouble, s(9).toDouble, s(10).toDouble, s(11).toDouble, s(12).toDouble, s(13).toDouble, s(14).toDouble, s(15).toDouble, s(16).toDouble, s(17).toDouble))}.cache
	                            
	val parsedCollectData = result.map{s1 => 
	                            val s = s1.substring(1, s1.length-1).split(",")
	                            LabeledPoint(s(5).toDouble, Vectors.dense(s(8).toDouble, s(9).toDouble, s(10).toDouble, s(11).toDouble, s(12).toDouble, s(13).toDouble, s(14).toDouble, s(15).toDouble, s(16).toDouble, s(17).toDouble))}.cache
	                            
	val parsedMorning = result.map{s1 => 
	                            val s = s1.substring(1, s1.length-1).split(",")
	                            LabeledPoint(s(6).toDouble, Vectors.dense(s(8).toDouble, s(9).toDouble, s(10).toDouble, s(11).toDouble, s(12).toDouble, s(13).toDouble, s(14).toDouble, s(15).toDouble, s(16).toDouble, s(17).toDouble))}.cache
	
	val parsedAfternoon = result.map{s1 => 
	                            val s = s1.substring(1, s1.length-1).split(",")
	                            LabeledPoint(s(7).toDouble, Vectors.dense(s(8).toDouble, s(9).toDouble, s(10).toDouble, s(11).toDouble, s(12).toDouble, s(13).toDouble, s(14).toDouble, s(15).toDouble, s(16).toDouble, s(17).toDouble))}.cache
###用随机森林模型开始预测
	import org.apache.spark.mllib.tree.RandomForest
	import org.apache.spark.mllib.tree.model.RandomForestModel
	// Train a RandomForest model.
	//分类数
	val numClasses = 2
	//为空表示所有特征为连续量
	val categoricalFeaturesInfo = Map[Int, Int]()
	//树的个数,实际中一般比3更多
	val numTrees = 50
	//特征子集采样的策略，auto是算法自助选择
	val featureSubsetStrategy = "auto" // Let the algorithm choose.
	val impurity = "variance"
	val maxDepth = 10
	val maxBins = 100
	
	val playModel = RandomForest.trainRegressor(parsedPlayData, categoricalFeaturesInfo,
	      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
	val downModel = RandomForest.trainRegressor(parsedDownData, categoricalFeaturesInfo,
	      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
	val collectModel = RandomForest.trainRegressor(parsedCollectData, categoricalFeaturesInfo,
	      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
	val morningModel = RandomForest.trainRegressor(parsedMorning, categoricalFeaturesInfo,
	      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
	val afternoonModel = RandomForest.trainRegressor(parsedAfternoon, categoricalFeaturesInfo,
	      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)  
###得出模型后，在训练数据上试试，看看误差怎么样
	// Evaluate model on test instances and compute test error
	val playLabelsAndPredictions = parsedPlayData.map { point =>
	      val prediction = playModel.predict(point.features)
	      (point.label, prediction)
	    }
	val downLabelsAndPredictions = parsedDownData.map { point =>
	      val prediction = downModel.predict(point.features)
	      (point.label, prediction)
	    }
	val collectLabelsAndPredictions = parsedCollectData.map { point =>
	      val prediction = collectModel.predict(point.features)
	      (point.label, prediction)
	    }
	val morningLabelsAndPredictions = parsedMorning.map { point =>
	      val prediction = morningModel.predict(point.features)
	      (point.label, prediction)
	    }
	val afternoonLabelsAndPredictions = parsedAfternoon.map { point =>
	      val prediction = afternoonModel.predict(point.features)
	      (point.label, prediction)
	    }
	    
	val playMSE = playLabelsAndPredictions.map { case (v, p) => math.pow((v - p), 2) }.mean()
	val downMSE = downLabelsAndPredictions.map { case (v, p) => math.pow((v - p), 2) }.mean()
	val collectMSE = collectLabelsAndPredictions.map { case (v, p) => math.pow((v - p), 2) }.mean()
	val morningMSE = morningLabelsAndPredictions.map { case (v, p) => math.pow((v - p), 2) }.mean()
	val afternoonMSE = afternoonLabelsAndPredictions.map { case (v, p) => math.pow((v - p), 2) }.mean()
	
	println("play Mean Squared Error = " + playMSE)
	println("down Mean Squared Error = " + downMSE)
	println("collect Mean Squared Error = " + collectMSE)
	println("morning Mean Squared Error = " + morningMSE)
	println("afternoon Mean Squared Error = " + afternoonMSE)
	//println("Learned regression forest model:\n" + model.toDebugString)
	
	println("playModel.algo:" + playModel.algo)
	println("playModel.trees:" + playModel.trees)
	println("downModel.algo:" + downModel.algo)
	println("downModel.trees:" + downModel.trees)
	println("collectModel.algo:" + collectModel.algo)
	println("collectModel.trees:" + collectModel.trees)

![](https://github.com/wlwgcdxc/picture/blob/master/15_03.PNG)	
从图中可以看到，在训练数据上还是挺好的，说明曲线拟合的比较好。因为有训练50棵树，所以过拟合的现象应该也不是很严重
###结果如下
	playLabelsAndPredictions.collect()
	downLabelsAndPredictions.collect()
	collectLabelsAndPredictions.collect()
	morningLabelsAndPredictions.collect()
	afternoonLabelsAndPredictions.collect()
![](https://github.com/wlwgcdxc/picture/blob/master/15_01.PNG)
从图中可以看出，在训练数据上还是挺不错的
###开始提取需要预测的数据（这里希望通过7.30和7.31的数据，预测出8月的数据）
	val result_temp = sc.textFile("/opt/xcdong/trycache/artistInfo_right_1/part-00000") 
	result_temp.first()
	result_temp.cache
	
	case class ArtistInfo_right_1(date: String, artist: String, gender: Int, todayPlay: Int, todayDown: Int, todayCollect: Int, todayMorning: Int, todayAfternoon: Int, yesPlay: Int, yesDown: Int, yesCollect: Int, playRate: Int, downRate: Int, collectRate: Int, yesMorning: Int, yesAfternoon: Int, publishTimes: Int, initialTimes: Int)
	
	val result_temp_1 = result_temp.map { e =>
	    val ele = e.substring(1, e.length - 1).split(",")
	    ArtistInfo_right_1(ele(0).toString, ele(1).toString, ele(2).toInt, ele(3).toInt, ele(4).toInt, ele(5).toInt, ele(6).toInt, ele(7).toInt, ele(8).toInt, ele(9).toInt, ele(10).toInt, ele(11).toInt, ele(12).toInt, ele(13).toInt, ele(14).toInt, ele(15).toInt, ele(16).toInt, ele(17).toInt)
	}.toDF
	result_temp_1.registerTempTable("artistInfo_right_1")
	
	val result_final = sqlContext.sql("select * from artistInfo_right_1 where date = '20150731' ").rdd.repartition(1).saveAsTextFile("/opt/xcdong/trycache/random_forest_7.31_8/artistInfo_0731")
	
	###开始使用模型预测
	val artistInfo_0731 = sc.textFile("/opt/xcdong/trycache/random_forest_7.31_8/artistInfo_0731")
	artistInfo_0731.first()
	artistInfo_0731.cache
	
	import java.io._
	//为了写文件
	object WriteToCSV extends Serializable{
	   val writer = new PrintWriter(new File("/opt/xcdong/trycache/random_forest_7.31_8/artist_predict"), "UTF-8") 
	   def getWriter(): PrintWriter = return writer
	   def closeWriter() {
	       writer.close()
	   } 
	}
	//为了拿到下一天的具体日期
	import java.text.ParsePosition
	import java.text.SimpleDateFormat
	import java.util.Calendar
	import java.util.Date
	import java.util.GregorianCalendar
	object Convert extends Serializable{
	    def findYesterday( date: String) : String = {
	      val sdf : SimpleDateFormat =new SimpleDateFormat("yyyyMMdd")
	      val st = sdf.parse(date, new ParsePosition(0))
	      val cal : GregorianCalendar  = new GregorianCalendar();
	      cal.setTime(st);
	      cal.add(Calendar.DAY_OF_MONTH, +1);
	      val result = sdf.format(cal.getTime());
	      return result;
	    } 
	}
	
	val artistInfo_0731_array = artistInfo_0731.collect()
	for (e: String <- artistInfo_0731_array) {
	    var ele = e.substring(1, e.length - 1).split(",")
	    var date = "20150731"
	    while (date != "20150830") {
	        date = Convert.findYesterday(ele(0).toString)
	        val artist = ele(1).toString
	        val gender = ele(2).toInt
	
	        val yesPlay = ele(3).toInt
	        val yesDown = ele(4).toInt
	        val yesCollect = ele(5).toInt
	        val playRate = ele(3).toInt - ele(8).toInt
	        val downRate = ele(4).toInt - ele(9).toInt
	        val collectRate = ele(5).toInt - ele(10).toInt
	        val yesMorning = ele(6).toInt
	        val yesAfternoon = ele(7).toInt 
	        val publishTimes = ele(16).toInt + 500
	        val initialTimes = ele(17).toInt
	       
	       
	        val prePlay = playModel.predict(Vectors.dense(yesPlay.toDouble, yesDown.toDouble, yesCollect.toDouble, playRate.toDouble, downRate.toDouble, collectRate.toDouble, yesMorning.toDouble, yesAfternoon.toDouble, publishTimes.toDouble, initialTimes.toDouble))
	        val preDown = downModel.predict(Vectors.dense(yesPlay.toDouble, yesDown.toDouble, yesCollect.toDouble, playRate.toDouble, downRate.toDouble, collectRate.toDouble, yesMorning.toDouble, yesAfternoon.toDouble, publishTimes.toDouble, initialTimes.toDouble))
	        val preCollect  = collectModel.predict(Vectors.dense(yesPlay.toDouble, yesDown.toDouble, yesCollect.toDouble, playRate.toDouble, downRate.toDouble, collectRate.toDouble, yesMorning.toDouble, yesAfternoon.toDouble, publishTimes.toDouble, initialTimes.toDouble))
	        val preMorning = morningModel.predict(Vectors.dense(yesPlay.toDouble, yesDown.toDouble, yesCollect.toDouble, playRate.toDouble, downRate.toDouble, collectRate.toDouble, yesMorning.toDouble, yesAfternoon.toDouble, publishTimes.toDouble, initialTimes.toDouble))
	        val preAfternoon = afternoonModel.predict(Vectors.dense(yesPlay.toDouble, yesDown.toDouble, yesCollect.toDouble, playRate.toDouble, downRate.toDouble, collectRate.toDouble, yesMorning.toDouble, yesAfternoon.toDouble, publishTimes.toDouble, initialTimes.toDouble))
	        
	        val str = new StringBuffer()
	        str.append(date)
	        str.append(",")
	        str.append(artist)
	        str.append(",")
	        str.append(gender)
	        str.append(",")
	        str.append(prePlay.toInt)
	        str.append(",")
	        str.append(preDown.toInt)
	        str.append(",")
	        str.append(preCollect.toInt)
	        str.append(",")
	        str.append(preMorning.toInt)
	        str.append(",")
	        str.append(preAfternoon.toInt)
	        str.append(",")
	        str.append(yesPlay)
	        str.append(",")
	        str.append(yesDown)
	        str.append(",")
	        str.append(yesCollect)
	        str.append(",")
	        str.append(playRate)
	        str.append(",")
	        str.append(downRate)
	        str.append(",")
	        str.append(collectRate)
	        str.append(",")
	        str.append(yesMorning)
	        str.append(",")
	        str.append(yesAfternoon)
	        str.append(",")
	        str.append(publishTimes)
	        str.append(",")
	        str.append(initialTimes)
	        WriteToCSV.getWriter.println(str)
	        ele = str.toString().split(",")
	    }
	}
	WriteToCSV.closeWriter()

###查看预测结果是否精准
	case class ArtistInfo_right_1(date: String, artist: String, gender: Int, todayPlay: Int, todayDown: Int, todayCollect: Int, todayMorning: Int, todayAfternoon: Int, yesPlay: Int, yesDown: Int, yesCollect: Int, playRate: Int, downRate: Int, collectRate: Int, yesMorning: Int, yesAfternoon: Int, publishTimes: Int, initialTimes: Int)
	
	val label = sc.textFile("/opt/xcdong/trycache/artistInfo_right_1") 
	label.first()
	label.cache
	
	val label_res = label.map { e =>
	    val ele = e.substring(1, e.length - 1).split(",")
	    ArtistInfo_right_1(ele(0).toString, ele(1).toString, ele(2).toInt, ele(3).toInt, ele(4).toInt, ele(5).toInt, ele(6).toInt, ele(7).toInt, ele(8).toInt, ele(9).toInt, ele(10).toInt, ele(11).toInt, ele(12).toInt, ele(13).toInt, ele(14).toInt, ele(15).toInt, ele(16).toInt, ele(17).toInt)
	}.toDF
	
	val predict = sc.textFile("/opt/xcdong/trycache/random_forest_7.31_8/artist_predict") 
	predict.first()
	predict.cache
	
	val predict_res = predict.map { e =>
	    val ele = e.split(",")
	    ArtistInfo_right_1(ele(0).toString, ele(1).toString, ele(2).toInt, ele(3).toInt, ele(4).toInt, ele(5).toInt, ele(6).toInt, ele(7).toInt, ele(8).toInt, ele(9).toInt, ele(10).toInt, ele(11).toInt, ele(12).toInt, ele(13).toInt, ele(14).toInt, ele(15).toInt, ele(16).toInt, ele(17).toInt)
	}.toDF
	
	val predict_result = label_res.join(predict_res, Seq("date", "artist"), "left_outer").select(label_res("date").as("date"), label_res("artist").as("artist"), label_res("todayPlay").as("label"), predict_res("todayPlay").as("predict"))
	predict_result.registerTempTable("predict_result")
	
	%sql
	select * 
	from predict_result 
	where predict > 0 and artist = "e087f8842fe66efa5ccee42ff791e0ca"
	order by date
###结果如图像所示(直观)
![](https://github.com/wlwgcdxc/picture/blob/master/1502.PNG)
从图中可以看出，前几天还比较相近，但是到了后几天，就完全不行了，基本后一天的记录和前一天的记录完全相同。说明通过预测值，再去连续预测，肯定是有问题的。
###总体标准差如下
![](https://github.com/wlwgcdxc/picture/blob/master/15_04.PNG)
结果比较差


#下面考虑使用（GBDT）预测，其实用随机森林也可以。GBDT的准确率应该更高，但是速度相比较RF慢多了，因为难并行化
###预测的方法是使用当天的数据去预测一个月，两个月之后的数据，然后使用7月份预测一个月之后的数据，6月份预测两个月之后的数据。然后，对他们预测出的8月份的数据做一个加权，得到8月份的数据

###找到某天一个月，两个月之后的日期，找到某天是星期几
	import java.text.ParsePosition
	import java.text.SimpleDateFormat
	import java.util.Calendar
	import java.util.Date
	import java.util.GregorianCalendar
	
	object Convert extends Serializable{
	    def findYesterday( date: String) : String = {//为了找到昨天的歌手播放量等信息
	      val sdf : SimpleDateFormat =new SimpleDateFormat("yyyyMMdd")
	      val st = sdf.parse(date, new ParsePosition(0))
	      val cal : GregorianCalendar  = new GregorianCalendar();
	      cal.setTime(st);
	      cal.add(Calendar.DAY_OF_MONTH, +1);
	      val result = sdf.format(cal.getTime());
	      return result;
	    }
	    
	    def findNextMonth(date: String) : String = {//为了找到下个月该歌手的播放量
	      val sdf : SimpleDateFormat =new SimpleDateFormat("yyyyMMdd")
	      val st = sdf.parse(date, new ParsePosition(0))
	      val cal : GregorianCalendar  = new GregorianCalendar();
	      cal.setTime(st);
	      cal.add(Calendar.DAY_OF_MONTH, -30);
	      val result = sdf.format(cal.getTime());
	      return result;
	    }
	    
	    def findNextTwoMonth(date: String) : String = {//为了找到下两个月该歌手的播放量
	      val sdf : SimpleDateFormat =new SimpleDateFormat("yyyyMMdd")
	      val st = sdf.parse(date, new ParsePosition(0))
	      val cal : GregorianCalendar  = new GregorianCalendar();
	      cal.setTime(st);
	      cal.add(Calendar.DAY_OF_MONTH, -60);
	      val result = sdf.format(cal.getTime());
	      return result;
	    }
	    
	    def findBeforeMonth(date: String) : String = {//为了找到下个月该歌手的播放量
	      val sdf : SimpleDateFormat =new SimpleDateFormat("yyyyMMdd")
	      val st = sdf.parse(date, new ParsePosition(0))
	      val cal : GregorianCalendar  = new GregorianCalendar();
	      cal.setTime(st);
	      cal.add(Calendar.DAY_OF_MONTH, +30);
	      val result = sdf.format(cal.getTime());
	      return result;
	    }
	    
	    def findBeforeTwoMonth(date: String) : String = {//为了找到下两个月该歌手的播放量
	      val sdf : SimpleDateFormat =new SimpleDateFormat("yyyyMMdd")
	      val st = sdf.parse(date, new ParsePosition(0))
	      val cal : GregorianCalendar  = new GregorianCalendar();
	      cal.setTime(st);
	      cal.add(Calendar.DAY_OF_MONTH, +60);
	      val result = sdf.format(cal.getTime());
	      return result;
	    }
	    
	    def findWeek(date: String) : Int = {//为了找到该天是星期几
	      val sdf : SimpleDateFormat =new SimpleDateFormat("yyyyMMdd")
	      val st = sdf.parse(date, new ParsePosition(0))
	      val cal : GregorianCalendar  = new GregorianCalendar();
	      cal.setTime(st);
	      val result = cal.get(Calendar.DAY_OF_WEEK)
	      return result;
	    }
	}
###读入原始表，并添加星期特征
	val artistInfo = sc.textFile("/opt/xcdong/trycache/artistInfo_right_1/part-00000")
	artistInfo.first
	case class Artist(date: String, artist: String, gender: Int, weekDay: Int, todayPlay: Int, todayDown: Int, todayCollect: Int, todayMorning: Int, todayAfternoon: Int, yesPlay: Int, yesDown: Int, yesCollect: Int, playRate: Int, downRate: Int, collectRate: Int, yesMorning: Int, yesAfternoon: Int, publishTimes: Int, initialTimes: Int)
	val artistDf = artistInfo.map { ele =>
	    val e = ele.substring(1, ele.length - 1).split(",")
	    Artist(e(0), e(1), e(2).toInt, Convert.findWeek(e(0).toString), e(3).toInt, e(4).toInt, e(5).toInt, e(6).toInt, e(7).toInt, e(8).toInt, e(9).toInt, e(10).toInt, e(11).toInt, e(12).toInt, e(13).toInt, e(14).toInt, e(15).toInt, e(16).toInt, e(17).toInt)
	}.toDF
###将测试数据和训练数据分开，训练数据为（3,4,5,6,7月的数据）,测试数据为（8月的数据）
	artistDf.registerTempTable("artist_all_info")
	val artist_1 = sqlContext.sql("select * from artist_all_info where date > '20150731' order by date ").rdd.repartition(1).saveAsTextFile("/opt/xcdong/trycache/GBDT/test_data_8")
	val artist_1 = sqlContext.sql("select * from artist_all_info where date < '20150801' order by date ").rdd.repartition(1).saveAsTextFile("/opt/xcdong/trycache/GBDT/train_data_3-7_ori")
###在训练数据上，找到一个月和两个月之后歌曲的播放量，作为两次预测（一月之后预测模型，二月之后预测模型）的标签
	val trainData = sc.textFile("/opt/xcdong/trycache/GBDT/train_data_3-7_ori")
	trainData.first
	case class Artist(date: String, artist: String, gender: Int, weekDay: Int, todayPlay: Int, todayDown: Int, todayCollect: Int, todayMorning: Int, todayAfternoon: Int, yesPlay: Int, yesDown: Int, yesCollect: Int, playRate: Int, downRate: Int, collectRate: Int, yesMorning: Int, yesAfternoon: Int, publishTimes: Int, initialTimes: Int)
	val trainDataDf_1 = trainData.map { ele =>
	    val e = ele.substring(1, ele.length - 1).split(",")
	    Artist(e(0), e(1), e(2).toInt, e(3).toInt, e(4).toInt, e(5).toInt, e(6).toInt, e(7).toInt, e(8).toInt, e(9).toInt, e(10).toInt, e(11).toInt, e(12).toInt, e(13).toInt, e(14).toInt, e(15).toInt, e(16).toInt, e(17).toInt, e(18).toInt)
	}.toDF
	trainDataDf_1.first
	//找到一月之后，两月之后歌曲的播放量
	trainDataDf_1.registerTempTable("trainDataDf_1")
	case class NextMonthInfo(date: String, nextMonth: String, artist: String, nextMonthPlay: Int)
	case class NextTwoMonthInfo(date: String, nextTwoMonth: String, artist: String, nextTwoMonthPlay: Int)
	val trainDataDf_2 = sqlContext.sql("select date, artist, todayPlay from trainDataDf_1").map { ele =>
	    NextMonthInfo(Convert.findNextMonth(ele.getString(0)), ele.getString(0), ele.getString(1), ele.getInt(2))
	}.toDF
	val trainDataDf_3 = sqlContext.sql("select date, artist, todayPlay from trainDataDf_1").map { ele =>
	    NextTwoMonthInfo(Convert.findNextTwoMonth(ele.getString(0)), ele.getString(0), ele.getString(1), ele.getInt(2))
	}.toDF
	//做表连接，将一月之后，两月之后的数据作为标签，拼接到训练数据上
	val trainDataDf_4 = trainDataDf_1.join(trainDataDf_2, Seq("date", "artist"), "left_outer").select(trainDataDf_1("date").as("date"), trainDataDf_1("artist").as("artist"), trainDataDf_2("nextMonth").as("nextMonth"), trainDataDf_2("nextMonthPlay").as("nextMonthPlay"), trainDataDf_1("gender").as("gender"), trainDataDf_1("weekDay").as("weekDay"), trainDataDf_1("todayPlay").as("todayPlay"), trainDataDf_1("todayDown").as("todayDown"),  trainDataDf_1("todayCollect").as("todayCollect"), trainDataDf_1("todayMorning").as("todayMorning"), trainDataDf_1("todayAfternoon").as("todayAfternoon"), trainDataDf_1("yesPlay").as("yesPlay"), trainDataDf_1("yesDown").as("yesDown"), trainDataDf_1("yesCollect").as("yesCollect"), trainDataDf_1("playRate").as("playRate"), trainDataDf_1("downRate").as("downRate"),  trainDataDf_1("collectRate").as("collectRate"), trainDataDf_1("yesMorning").as("yesMorning"), trainDataDf_1("yesAfternoon").as("yesAfternoon"), trainDataDf_1("publishTimes").as("publishTimes"), trainDataDf_1("initialTimes").as("initialTimes"))
	trainDataDf_4.first
	
	val trainDataDf_5 = trainDataDf_4.join(trainDataDf_3, Seq("date", "artist"), "left_outer").select(trainDataDf_4("date").as("date"), trainDataDf_4("artist").as("artist"), trainDataDf_4("nextMonth").as("nextMonth"), trainDataDf_4("nextMonthPlay").as("nextMonthPlay"), trainDataDf_3("nextTwoMonth").as("nextTwoMonth"), trainDataDf_3("nextTwoMonthPlay").as("nextTwoMonthPlay"), trainDataDf_4("gender").as("gender"), trainDataDf_4("weekDay").as("weekDay"), trainDataDf_4("todayPlay").as("todayPlay"), trainDataDf_4("todayDown").as("todayDown"),  trainDataDf_4("todayCollect").as("todayCollect"), trainDataDf_4("todayMorning").as("todayMorning"), trainDataDf_4("todayAfternoon").as("todayAfternoon"), trainDataDf_4("yesPlay").as("yesPlay"), trainDataDf_4("yesDown").as("yesDown"), trainDataDf_4("yesCollect").as("yesCollect"), trainDataDf_4("playRate").as("playRate"), trainDataDf_4("downRate").as("downRate"),  trainDataDf_4("collectRate").as("collectRate"), trainDataDf_4("yesMorning").as("yesMorning"), trainDataDf_4("yesAfternoon").as("yesAfternoon"), trainDataDf_4("publishTimes").as("publishTimes"), trainDataDf_4("initialTimes").as("initialTimes"))
	trainDataDf_5.first
	//保存收据
	trainDataDf_5.rdd.repartition(1).saveAsTextFile("/opt/xcdong/trycache/GBDT/train_data_3-7_ori_tmp")
	//去掉包含的null值
	val train_data_result = sc.textFile("/opt/xcdong/trycache/GBDT/train_data_3-7_ori_tmp").map( s => s.replace("null", "0")).repartition(1).saveAsTextFile("/opt/xcdong/trycache/GBDT/train_data_3-7_end")
###准备数据
	import org.apache.spark.mllib.regression.LabeledPoint
	import org.apache.spark.mllib.linalg.Vectors
	val data = sc.textFile("/opt/xcdong/trycache/GBDT/train_data_3-7_end").map(ele => ele.substring(1, ele.length-1).split(","))
	//一月之后歌曲播放量的模型需要的数据
	val oneMonthAfterData = data.filter(e => e(0)<"20150701").map(s => LabeledPoint(s(3).toDouble, Vectors.dense(s(6).toDouble, s(7).toDouble, s(8).toDouble, s(9).toDouble, s(10).toDouble, s(11).toDouble, s(12).toDouble, s(13).toDouble, s(14).toDouble, s(15).toDouble, s(16).toDouble, s(17).toDouble, s(18).toDouble, s(19).toDouble, s(20).toDouble, s(21).toDouble, s(22).toDouble))).cache
	//两月之后歌曲播放量的模型需要的数据
	val twoMonthAfterData = data.filter(e => e(0)<"20150601").map(s => LabeledPoint(s(5).toDouble, Vectors.dense(s(6).toDouble, s(7).toDouble, s(8).toDouble, s(9).toDouble, s(10).toDouble, s(11).toDouble, s(12).toDouble, s(13).toDouble, s(14).toDouble, s(15).toDouble, s(16).toDouble, s(17).toDouble, s(18).toDouble, s(19).toDouble, s(20).toDouble, s(21).toDouble, s(22).toDouble))).cache
###训练模型
	import org.apache.spark.mllib.tree.GradientBoostedTrees
	import org.apache.spark.mllib.tree.configuration.BoostingStrategy
	import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
	import org.apache.spark.mllib.util.MLUtils
	
	//通常GRDT使用多个深度小的树，进行预测，效果比较好
	// The defaultParams for Regression use SquaredError by default.
	//boostingStrategy.numIterations = 3 //这个默认的是100，也就是会产生100个树
	//boostingStrategy.learningRate = 0.1 //学习率默认的是0.1
	val boostingStrategy = BoostingStrategy.defaultParams("Regression")
	boostingStrategy.treeStrategy.maxDepth = 5
	boostingStrategy.treeStrategy.numClasses = 2
	boostingStrategy.numIterations = 330
	// Empty categoricalFeaturesInfo indicates all features are continuous.
	boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]((0, 4), (1, 8))
	val oneMonthAfterModel = GradientBoostedTrees.train(oneMonthAfterData, boostingStrategy)
	// 看看在训练数据上拟合的怎么样
	val oneMonthLabelsAndPredictions = oneMonthAfterData.map { point =>
	  val prediction = oneMonthAfterModel.predict(point.features)
	  (point.label, prediction)
	}
	val oneMonthTestMSE = oneMonthLabelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2)}.mean()
	println("after one month, Test Mean Squared Error = " + oneMonthTestMSE)
	//println("Learned regression GBT model:\n" + oneMonthAfterModel.toDebugString)
	
	//训练两月之后歌曲的播放量的模型
	val twoMonthAfterModel = GradientBoostedTrees.train(twoMonthAfterData, boostingStrategy)
	val twoMonthLabelsAndPredictions = twoMonthAfterData.map { point =>
	  val prediction = twoMonthAfterModel.predict(point.features)
	  (point.label, prediction)
	}
	val twoMonthTestMSE = twoMonthLabelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2)}.mean()
	println("after two month, Test Mean Squared Error = " + twoMonthTestMSE)

###预测实际值
	import org.apache.spark.mllib.regression.LabeledPoint
	import org.apache.spark.mllib.linalg.Vectors
	
	case class PredictArtistInfo(date: String, artist: String, predict: Int)
	case class RealArtistInfo(date: String, artist: String, playTimes: Int)
	val data = sc.textFile("/opt/xcdong/trycache/GBDT/train_data_3-7_end").map(ele => ele.substring(1, ele.length-1).split(","))
	
	val _7to8 = data.filter(e => e(0)>"20150630").map { s =>
	    val prediction = oneMonthAfterModel.predict(Vectors.dense(s(6).toDouble, s(7).toDouble, s(8).toDouble, s(9).toDouble, s(10).toDouble, s(11).toDouble, s(12).toDouble, s(13).toDouble, s(14).toDouble, s(15).toDouble, s(16).toDouble, s(17).toDouble, s(18).toDouble, s(19).toDouble, s(20).toDouble, s(21).toDouble, s(22).toDouble))
		(Convert.findBeforeMonth(s(0)), s(1), prediction)
	}.map { e =>
	    PredictArtistInfo(e._1, e._2, e._3.toInt)
	}.toDF
	_7to8.collect
	val _6to8 = data.filter(e => e(0)>"20150520" && e(0)<"20150720").map { s =>
		val prediction = twoMonthAfterModel.predict(Vectors.dense(s(6).toDouble, s(7).toDouble, s(8).toDouble, s(9).toDouble, s(10).toDouble, s(11).toDouble, s(12).toDouble, s(13).toDouble, s(14).toDouble, s(15).toDouble, s(16).toDouble, s(17).toDouble, s(18).toDouble, s(19).toDouble, s(20).toDouble, s(21).toDouble, s(22).toDouble))
		(Convert.findBeforeTwoMonth(s(0)), s(1), prediction)	
	}.map { e =>
	    PredictArtistInfo(e._1, e._2, e._3.toInt)
	}.toDF
	_6to8.collect
	val predict_result = _7to8.join(_6to8, Seq("date", "artist"), "left_outer").select(_7to8("*"), _6to8("predict").as("predict_twoMonth")).map { e =>
	    val mean = (e.getInt(2) + e.getInt(3)) / 2
		(e.getString(0), e.getString(1), mean)
	}.map { e =>
	    PredictArtistInfo(e._1, e._2, e._3)
	}.toDF
	predict_result.rdd.repartition(1).saveAsTextFile("/opt/xcdong/trycache/GBDT/predict_data_8")
	predict_result.collect
###和8月实际的播放量相比较，看看效果怎么样	
	case class PredictArtistInfo(date: String, artist: String, predict: Int)
	case class RealArtistInfo(date: String, artist: String, playTimes: Int)
	
	val predict_data = sc.textFile("/opt/xcdong/trycache/GBDT/predict_data_8").map(ele => ele.substring(1, ele.length-1).split(",")).map { e =>
	    PredictArtistInfo(e(0), e(1), e(2).toInt)
	}.toDF
	println("haha")
	val real_data = sc.textFile("/opt/xcdong/trycache/GBDT/test_data_8").map(ele => ele.substring(1, ele.length-1).split(",")).map { e =>
	    RealArtistInfo(e(0), e(1), e(4).toInt)
	}.toDF
	println("heihei")
	val contrast = real_data.join(predict_data, Seq("date", "artist"), "left_outer").select(real_data("*"), predict_data("predict"))
	contrast.rdd.repartition(1).saveAsTextFile("/opt/xcdong/trycache/GBDT/contrast_data_8")
	contrast.map{ e => math.pow((e.getInt(2) - (if (e.get(3) == null) 0 else e.getInt(3))), 2)}.mean()
	contrast.registerTempTable("predict")	
###结果如下
![](https://github.com/wlwgcdxc/picture/blob/master/GBDT_1.PNG)
###相当于每个歌手每天播放量的误差在280首左右，效果还不是很尽人意。可借由下面的图，分析下原因
	%sql
	select * from predict where artist = "c5f0170f87a2fbb17bf65dc858c745e2" or artist = "099cd99056bf92e5f0e384465890a804" or artist = "3964ee41d4e2ade1957a9135afe1b8dc" or artist = "2e14d32266ee6b4678595f8f50c369ac"
![](https://github.com/wlwgcdxc/picture/blob/master/GBDT_2.PNG)

![](https://github.com/wlwgcdxc/picture/blob/master/GBDT_3.png)
	%sql
	select * from predict where artist = "8fb3cef29f2c266af4c9ecef3b780e97" or artist = "7e0db58c13d033dafe5f5e1e70ff7eb4"
![](https://github.com/wlwgcdxc/picture/blob/master/GBDT_4.PNG)	
###可以看到歌手播放量比较小时，拟合的比较好。要是播放量比较大，误差就比较大了。
#下面可以考虑使用聚类对歌手进行聚类，同一类的歌手使用同一个预测模型，可能效果会更好些。然后就是，造成上述原因，还有可能是数据量太少，加大数据量再试试。同时增长率那个特征，可以考虑使用15天之前的数据做增长量，更合理些。
