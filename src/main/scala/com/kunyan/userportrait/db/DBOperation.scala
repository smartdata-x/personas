package com.kunyan.userportrait.db

import java.sql.{DriverManager, PreparedStatement}

import org.apache.spark.sql.SQLContext

/**
  * Created by C.J.YOU on 2016/4/26.
  */
object DBOperation extends Serializable{

  val url = "jdbc:mysql://:3306/personas?user=personas&password=personas&useUnicode=true&characterEncoding=utf8"
  val driver = "com.mysql.jdbc.Driver"
  val username = "personas"
  val password = "personas"
  Class.forName(driver)
  val connection = DriverManager.getConnection(url, username, password)

  def batchInsert(columns:Array[String], iterator: Iterable[(String, String,String)]): Unit = {
    //var connection: Connection = null
    var ps: PreparedStatement = null
    val sql ="insert into main_index (" + columns (0) + "," + columns (1) + "," + columns (2) + ") values (?,?,?)"
    ps = connection.prepareStatement(sql)
    try {
      // conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark", "root", "123456")
      iterator.foreach(data => {
        ps.setString (1,data._1)
        ps.setString (2,data._2)
        ps.setString (3,data._3)
        ps.addBatch()
      })
      ps.executeBatch()
    } catch {
      case e: Exception => println("Mysql Exception")
    } finally {
      if (ps != null) {
        ps.close()
      }
    }
  }

  def batchUpdate(columns:Array[String], iterator: Iterable[(String, String,String)]): Unit ={
    var ps: PreparedStatement = null
    val sql = "update main_index set " + columns (0) + " = ?," + columns (1) + "= ? where id = ?"
    try {
      iterator.foreach(data => {
        ps = connection.prepareStatement(sql)
        ps.setString(1,data._1)
        ps.setString (2,data._2)
        ps.setInt(3,data._3.toInt)
        ps.addBatch()
      })
      ps.executeBatch()
    } catch {
      case e: Exception => println("Mysql Exception")
    } finally {
      if (ps != null) {
        ps.close()
      }
    }
  }

  private def updataColumn(sqlContext:SQLContext, col: Array[String],data:Array[String]): Unit ={
    println("updataColumn:"+data)
    try {
      val updateSql = "update main_index set " + col (0) + " = ?," + col (1) + "= ? where id = ?"
      val updatePs = connection.prepareStatement (updateSql)
      DBOperation.update (updatePs, data)
      updatePs.executeBatch ()
      updatePs.close ()
    } catch {
      case e:Exception =>
        println("updataColumn ERROR")
    }
  }

  private def insertColumn(sqlContext:SQLContext,columns:Array[String],data: Array[(String)]): Unit ={
    println("insertColumn:"+data)
    try {
      val insertSql = "insert into main_index (" + columns (0) + "," + columns (1) + "," + columns (2) + ") values (?,?,?)"
      val insertPs = connection.prepareStatement (insertSql)
      DBOperation.insert (insertPs, data)
      insertPs.executeBatch ()
      insertPs.close ()
    } catch {
      case e:Exception =>
        println("insertColumn ERROR")
    }
  }
  private def saveData(sqlContext:SQLContext,columns:Array[String],data: Array[(String,Int)]): Unit ={
    val updateSql = "update main_index set "+columns(0)+" = ?,"+columns(1)+" = ?  where id = ?"
    val insertSql = "insert into main_index ("+columns(0)+","+columns(1)+") values (?,?)"
    val updatePs = connection.prepareStatement(updateSql)
    val insertPs = connection.prepareStatement(insertSql)
      data.foreach { x =>
        val dataFrame = sqlContext.sql("select id,"+columns(1)+" from main_index where "+columns(0)+" ='" + x._1 + "'")
        // println("dataFram:"+dataFrame.show(1))
        if (dataFrame.count > 0) {
          // println("count_phone:"+dataFrame.take(1)(0)(1).toString)
          val id = dataFrame.take(1)(0)(0).toString
          val count = dataFrame.take(1)(0)(1).toString
          if (Integer.parseInt(count) < x._2) {
            DBOperation.update(updatePs, Array(x._1, x._2.toString,id))
          }
        } else {
          //println("insert:data")
          DBOperation.insert(insertPs, Array(x._1, x._2.toString))
        }
      }
      updatePs.executeBatch()
      insertPs.executeBatch()
      updatePs.close()
      insertPs.close()
  }

  private def update(ps:PreparedStatement,array: Array[String]): Unit ={
      ps.setString(1,array(0))
      ps.setString (2,array(1))
      ps.setInt (3,array(2).toInt)
      /*ps.setString (3, array(2))
      ps.setInt (4, array(3).toInt)
      ps.setString (5, array(4))
      ps.setInt (6, array(5).toInt)*/
      ps.addBatch()
  }
  private def insert(ps:PreparedStatement,array: Array[String]): Unit ={
      ps.setString (1,array(0))
      ps.setString (2,array(1))
      ps.setString (3,array(2))
      /*ps.setString (3, array(2))
      ps.setInt (4, array(3).toInt)
      ps.setString (5, array(4))
      ps.setInt (6, array(5).toInt)*/
      ps.addBatch()

  }

}
