package com.weather.bigdata.it.spark.platform.split

import java.net.URI

import com.alibaba.fastjson.JSONObject
import com.weather.bigdata.it.utils.hdfsUtil.HDFSConfUtil
import com.weather.bigdata.it.utils.operation.JsonOperation
import org.apache.hadoop.fs.Path

object Analysis {
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

  def analysisJsonProperties(jsonStr: String): (String, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double) = {
    val myjoson: JSONObject = JsonOperation.Str2JSONObject(jsonStr)
    this.analysisJsonProperties(myjoson)
  }

  def analysisJsonIdName(JSONPro: JSONObject): String = this.analysisJsonProperties(JSONPro)._1

  def analysisJsonProperties(JSONPro: JSONObject): (String, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double) = {
    val idName: String = JSONPro.getString(this.idNameKey)
    val (timeBegin, timeEnd, timeStep) = (JSONPro.getString(this.time_BeginKey).toDouble, JSONPro.getString(this.time_EndKey).toDouble, JSONPro.getString(this.time_StepKey).toDouble)
    val (heightBegin, heightEnd, heightStep) = (JSONPro.getString(this.height_BeginKey).toDouble, JSONPro.getString(this.height_EndKey).toDouble, JSONPro.getString(this.height_StepKey).toDouble)
    val (latBegin, latEnd, latStep) = (JSONPro.getString(this.lat_BeginKey).toDouble, JSONPro.getString(this.lat_EndKey).toDouble, JSONPro.getString(this.lat_StepKey).toDouble)
    val (lonBegin, lonEnd, lonStep) = (JSONPro.getString(this.lon_BeginKey).toDouble, JSONPro.getString(this.lon_EndKey).toDouble, JSONPro.getString(this.lon_StepKey).toDouble)
    (idName, timeBegin, timeEnd, timeStep, heightBegin, heightEnd, heightStep, latBegin, latEnd, latStep, lonBegin, lonEnd, lonStep)
  }

  def getRegion(splitFile:String): String ={
    val uri = URI.create(splitFile)
    val pathName=new Path(uri)
    val kkk={
      if(HDFSConfUtil.isLocal(splitFile)){
        pathName.getParent.getName
      }else{
        pathName.getName
      }
    }
    kkk.split("\\.").head
  }
}
