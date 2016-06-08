### big-data-competition_dxc
================================

##### 解析文本
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

#进一步处理
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

