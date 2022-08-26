//--------------------------------------------------------------------------------------
//Problem statement : Calculates statistics from humidity sensor data..
//Developed date : 05-08-2022
//--------------------------------------------------------------------------------------
package org.luxoft.humidity
import java.io.File
import scala.io.Source
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.LinkedHashSet


case class telemetryValueMap(SensorId : String, telemetryValue : String)
    
object statistics {
  
 //main method
 def main(args: Array[String]): Unit = {
     
     val file = new File("src/resources/");
     val listFile =  file.listFiles.filter(_.isFile).map(_.getPath).toList;
     
     var measurementsProcessedMap = new HashMap[String,Int]();
     var measurementsFailedMap = new HashMap[String,Int]();
     var processedFilesMap = new HashMap[String,Int]();
     var rawProcessedMap = new HashMap[String,Int]();
     var listNanTelemetryBuffer = new HashMap[String,String]();
     var listProcessedData = new HashMap[String,List[(String,Int)]]();
     val linkedHashSet = scala.collection.mutable.LinkedHashSet[String]();
     
     var measurementsProcessedCounter = 1;
     var measurementsFailedCounter = 1;
     var incrementCounter = 0;
     
     var listTelemetryBuffer = new ListBuffer[(String,Int)]()
   
     listFile.foreach(filePath => {
         val fileExtensionType = filePath.toString.split("\\.").last 
         if(fileExtensionType.equals("csv")){
            val list = Source.fromFile(filePath).getLines().drop(1).toList; //reading files.
            list.map(processData)
            incrementCounter = incrementCounter + 1;
            processedFilesMap += ("processedFiles" -> incrementCounter);
         }
    })
      
    //calculate the telemetry statistics data
    def processData(sensorReadingContents : String) : Unit = {
       val telemetryInfoArray = sensorReadingContents.split(",")
       val sensorId = telemetryInfoArray(0).toLowerCase();
       val telemetryValue = telemetryInfoArray(1);
       if(!telemetryValue.toUpperCase().contains("NAN")){
          listTelemetryBuffer.append((sensorId, telemetryValue.toInt));
       }
       else{
         listNanTelemetryBuffer += ( sensorId -> "NAN");
       }
     
       
       if(measurementsProcessedMap.contains("ProcessedMeasurements")){
          val processedValue = measurementsProcessedMap("ProcessedMeasurements");
          measurementsProcessedMap.put("ProcessedMeasurements",processedValue + 1);
        }
        else{
           measurementsProcessedMap.put("ProcessedMeasurements", 1);
        }
        if(telemetryValue.toUpperCase().contains("NAN")){
          if(measurementsFailedMap.contains("FailedMeasurements")){
             val failedValue = measurementsFailedMap("FailedMeasurements")
             measurementsFailedMap.put("FailedMeasurements",failedValue + 1);
          }
          else{  measurementsFailedMap.put("FailedMeasurements",1);}}
          measurementsProcessedCounter = measurementsProcessedCounter + 1;
     }
     
     
     
     //sensor telemetry insights calculation
     val maxTelemtryValueBySensorMap = listTelemetryBuffer.groupBy(_._1).map { case (key, value) => key -> 
                                                                             value.map { _._2}.max}
     val minTelemetryValueBySensorMap = listTelemetryBuffer.groupBy(_._1).map { case (key, value) => key -> 
                                                                              value.map { _._2}.min}
     val averageTelemetryValueBySensorMap = listTelemetryBuffer.groupBy(_._1).map { case (key, value) => key -> 
                                                                              value.map { _._2}.sum / value.map { _._2}.size}
     val finalSortedAverageMap =  averageTelemetryValueBySensorMap.toList.sortWith(_._2 > _._2);
     val finalProcessedMap = finalSortedAverageMap.++(minTelemetryValueBySensorMap.toList).++(maxTelemtryValueBySensorMap.toList)
     
     val sortedList = finalProcessedMap.sorted
     val finalList = sortedList.groupBy(_._1).map { case (k, v) => k -> v.map { _._2}.mkString(",")}
     
     println("--------------------------------Final Telemetry Processed Output-----------------------------------------------------")
     if(!processedFilesMap.isEmpty){
        println("No Of Processed Files " + processedFilesMap.get("processedFiles").get);
     }
    if(!measurementsFailedMap.isEmpty){
        println("No Of Failed Measurements " + measurementsFailedMap.get("FailedMeasurements").get);
     }
     if(!measurementsProcessedMap.isEmpty){
          println("No Of Processed Measurements " + measurementsProcessedMap.get("ProcessedMeasurements").get);
     }
    
    //----------------------------------------------------------------------------------------------------------
     println("\n");
     println("sensor-id,min,avg,max");
     for(i <- finalSortedAverageMap){
        val filterValue = finalList.filter(x=>x._1 == i._1);
        val stringBuilder = i._1 +"," +filterValue.get(i._1).mkString(",");
        println(stringBuilder);
     }
     //"----------------------------------------Fetching  NAN Values---------------------------------------------
     for(i <- listNanTelemetryBuffer.toList){
       val nanFilter = finalList.filter(x=>x._1 == i._1);
       if(nanFilter.isEmpty){
         val stringBuilder = i._1+","+"NAN"+","+"NAN"+","+"NAN";
         println(stringBuilder);
       }
     }
     println("-------------------------------------------------------------------------------------")
  
     
   }
}
