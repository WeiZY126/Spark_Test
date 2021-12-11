package com.study.test

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

object Executor2 {
  def main(args: Array[String]): Unit = {
    //启动服务器，来接受数据
    val server = new ServerSocket(8888)
    println("服务器启动，等待接收数据")

    //等待客户端的链接
    val clientLink: Socket = server.accept()

    val in: InputStream = clientLink.getInputStream
    val objIn: ObjectInputStream = new ObjectInputStream(in)
    val subTask: SubTask = objIn.readObject().asInstanceOf[SubTask]
    val ints: List[Int] = subTask.compute()
    println("[8888]计算节点计算的结果为" + ints)

    in.close()
    clientLink.close()
    server.close()


  }

}
