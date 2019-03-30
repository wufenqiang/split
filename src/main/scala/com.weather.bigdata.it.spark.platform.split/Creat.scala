package com.weather.bigdata.it.spark.platform.split

import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.weather.bigdata.it.utils.hdfsUtil.HDFSReadWriteUtil
import com.weather.bigdata.it.utils.operation.{ArrayOperation, JsonOperation}

object Creat {
  private val idNameKey=Constant.idNameKey

  private val time_BeginKey=Constant.time_BeginKey
  private val time_EndKey=Constant.time_EndKey
  private val time_StepKey=Constant.time_StepKey

  private val height_BeginKey=Constant.height_BeginKey
  private val height_EndKey=Constant.height_EndKey
  private val height_StepKey=Constant.height_StepKey

  private val lat_BeginKey=Constant.lat_BeginKey
  private val lat_EndKey=Constant.lat_EndKey
  private val lat_StepKey=Constant.lat_StepKey

  private val lon_BeginKey=Constant.lon_BeginKey
  private val lon_EndKey=Constant.lon_EndKey
  private val lon_StepKey=Constant.lon_StepKey


  def creatJsonFile(fileName:String,SplitLat:Int,SplitLon:Int,LatLon:String,SplitTime:Int=1,SplitHeight:Int=1): Unit ={
    val splitNum = SplitLat * SplitLon * SplitTime * SplitHeight
    val splitJson = this.creatJson(SplitLat,SplitLon,LatLon,SplitTime,SplitHeight)
    val flag=HDFSReadWriteUtil.writeTXT(fileName,splitJson.toString,false)
    System.out.println("输出文件地址:" + fileName+"(flag="+flag+")")
  }

  def creatJson(SplitLat:Int,SplitLon:Int,LatLon:String,SplitTime:Int=1,SplitHeight:Int=1): JSONArray ={

    val splitNum = SplitLat * SplitLon * SplitTime * SplitHeight

    val time = ArrayOperation.ArithmeticArray(3d, 240d, 3d)
    val height = ArrayOperation.ArithmeticArray(0d, 1d, 1d)

    val latlon = LatLon.split(",")
    val latBegin = latlon(0).toDouble
    val latEnd = latlon(1).toDouble
    val lonBegin = latlon(2).toDouble
    val lonEnd = latlon(3).toDouble

    val SplitNum = SplitTime*SplitHeight*SplitLat * SplitLon

    val dlat = latEnd - latBegin
    val dlon = lonEnd - lonBegin
    val dtime = time(time.length - 1) - time(0)
    val dheight = height(height.length - 1) - height(0)

    val values = new Array[Array[String]](SplitNum)
    val users = new Array[JSONObject](SplitNum)

//    val keys = Array[String]("Name", "time_Begin", "time_End", "time_Step", "height_Begin", "height_End", "height_Step", "lat_Begin", "lat_End", "lat_Step", "lon_Begin", "lon_End", "lon_Step")
  val keys = Array[String](this.idNameKey, this.time_BeginKey, this.time_EndKey,this.time_StepKey,this.height_BeginKey,this.height_EndKey,this.height_StepKey,this.lat_BeginKey,this.lat_EndKey,this.lat_StepKey,this.lon_BeginKey,this.lon_EndKey,this.lon_StepKey)

    var n:Int=0
    for(i<- 0 to SplitLat-1){
      for(j<- 0 to SplitLon-1){
        values(n) = Array[String](String.valueOf(n), "3", "240", "3", "0", "0", "1", String.valueOf(i * dlat / SplitLat + latBegin), String.valueOf((i + 1) * dlat / SplitLat + latBegin), "0.05d", String.valueOf(j * dlon / SplitLon + lonBegin), String.valueOf((j + 1) * dlon / SplitLon + lonBegin), "0.05d")
        users(n) = JsonOperation.createJSONObject(keys, values(n))
        n += 1
      }
    }
    JsonOperation.createJSONArray(users)
  }
  def main(args:Array[String]): Unit ={
    val LatLon = "0,60,70,140"
    val fileName="/tmp/JsonProperties.txt"
    this.creatJsonFile(fileName,6,7,LatLon)
  }
}
