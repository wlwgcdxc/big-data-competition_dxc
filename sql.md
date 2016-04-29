# *big-data-competition_dxc*
======================================
case class SongInfo(songid: String, artistid: String, publishTime: String, initialPlayTimes: Int, language: Int, gender: Int)  //定义样例类
val songText = sc.textFile("/opt/meizhang/trycache/mars_tianchi_songs.csv") //将格式化文本转换为RDD，其中文本的每一行就是RDD中的一项纪录
val songinfo = songText.map( s => s.split(",")).filter(x =>x.length==6).map(s=>SongInfo(s(0), s(1), s(2), s(3).toInt, s(4).toInt, s(5).toInt)) //RDD的transformation，将RDD中存储的string类型转换为定义的样例类SongInfo
val song = songinfo.toDF  //将RDD转换为dataframe
song.show()
val res = song.registerTempTable("songinfo") //将DF注册成一个名为“songinfo”的临时表，可以使用sql语句在表中查询， eg： select * from songinfo
/*同理，将useraction的信息读进来，也注册为表*/
case class UsersAction(userid: String, songid: String, playTime: String, actionType: Int, date: String)
val usersActionText = sc.textFile("/opt/meizhang/trycache/test_mars_tianchi_user_actions.csv")
val useractionInfo = usersActionText.map(s => s.split(",")).filter(x => x.length==5).map(s=> UsersAction(s(0), s(1),s(2),s(3).toInt,s(4)) )

val rawUserAction = useractionInfo.toDF.registerTempTable("rawUserAction")  //包含重复数据
val useraction = sqlContext.sql("select distinct *  from rawUserAction").registerTempTable("useraction") //去除重复数据

//将两张表拼接成为一张大表，sqlContext.sql返回DF
val allInfo = sqlContext.sql("select date, userid, s.songid, actionType, playTime, artistid, publishtime, initialPlayTimes, language, gender from useraction as u left join songinfo as s on u.songid=s.songid") 

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

### 统计播放量 下载量  收藏量（同时，记录昨天的数据，前天的数据）

//需要重新定义一个case class,用来分别统计歌曲的播放量，下载量和收藏量，也就是将actionType分开统计
case class RealInfo(date: String, songid: String, broadcast: Int, download: Int, collect: Int)
import org.apache.spark.sql._
val today = allInfo.groupBy("date", "songid", "actionType").count  //dataframe
//首先将today转换为RDD
val todayRDD = today.map(x => (x.getString(0), x.getString(1), x.getInt(2), x.getLong(3).toInt))
val yesterdayRDD = todayRDD.map(x => ( Convert.findYesterday(x._1), x._2, x._3, x._4))//有点难理解，（3.14 23 15 1） （3.15 25 16 3）=》（3.15的昨天 23 15 1）这一天的昨天 是这么多记录 
val agoRDD = yesterdayRDD.map(x => ( Convert.findYesterday(x._1), x._2, x._3, x._4))  
val yesterdayDF =  yesterdayRDD.map(x => RealInfo(x._1, x._2, if(x._3==1)x._4 else 0, if(x._3==2) x._4 else 0,if(x._3==3) x._4 else 0)).toDF
val agoDF = agoRDD.map(x => RealInfo(x._1, x._2, if(x._3==1)x._4 else 0, if(x._3==2) x._4 else 0,if(x._3==3) x._4 else 0)).toDF
val todayDF = todayRDD.map(x => RealInfo(x._1, x._2, if(x._3==1)x._4 else 0, if(x._3==2) x._4 else 0,if(x._3==3) x._4 else 0)).toDF//有点难理解，每行数据，变成了这种（3.15 离歌 3 0 0 0）（3.15 离歌 0 4 0 ）（3.15 离歌 0 0 5） 之后需要把这些合并在一起，变成（3.15 离歌 3 4 5）
todayDF.registerTempTable("todayDF")
yesterdayDF.registerTempTable("yesterdayDF")
agoDF.registerTempTable("agoDF")

### //合并 （3.15 离歌 3 0 0 0）（3.15 离歌 0 4 0 ）（3.15 离歌 0 0 5） 之后需要把这些合并在一起，变成（3.15 离歌 3 4 5）
val todayCollect = sqlContext.sql("select date,songid, sum(broadcast) as playTime,sum(download) as download,sum(collect) as collect from todayDF group by date,songid order by date")
val yesterdayCollect = sqlContext.sql("select date,songid, sum(broadcast) as playTime,sum(download) as download,sum(collect) as collect from yesterdayDF group by date,songid order by date")
val agoCollect = sqlContext.sql("select date,songid, sum(broadcast) as playTime,sum(download) as download,sum(collect) as collect from agoDF group by date,songid order by date")
todayCollect.registerTempTable("todayCollect")
yesterdayCollect.registerTempTable("yesterdayCollect")
agoCollect.registerTempTable("agoCollect")

### //将今天，昨天，后天的数据统计到一个表里
//今天的表跟昨天的表join，生成join1  dataFrame
val join1 = todayCollect.join(yesterdayCollect, Seq("date", "songid"), "left_outer").select(todayCollect("*"), yesterdayCollect("playTime").as("yesterdayPlay"), yesterdayCollect("download").as("yesterdayDown"), yesterdayCollect("collect").as("yesterdayC")) 
//前两天的大表与前三天的表左连接
val joinFinally = join1.join(agoCollect,Seq("date", "songid"), "left_outer").select(join1("*"), agoCollect("playTime").as("agoPlay"), agoCollect("download").as("agoDown"), agoCollect("collect").as("agoC")).registerTempTable("joinFinally")

### //将统计出的信息与包含歌曲全部信息的allinfo表拼接，将播放量，下载量，收藏量作为标签，即（要预测的量）
val allSongInfo = sqlContext.sql("select jf.date, jf.songid, al.playTime, download, collect, yesterdayPlay, yesterdayDown, yesterdayC, agoPlay, agoDown, agoC, artistid, publishtime, initialPlayTimes, language, gender from joinFinally as jf, allInfo as al where jf.songid=al.songid and jf.date=al.date order by jf.date").registerTempTable("allSongInfo")
val finalResult = sqlContext.sql("select * from allSongInfo").rdd.repartition(1).saveAsTextFile("/opt/meizhang/trycache/result_test_final") //这个dataframe包含某天某首歌今天，昨天，前天的播放量，下载量，收藏量, 
