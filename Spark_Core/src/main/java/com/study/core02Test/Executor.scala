package com.study.core02Test

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

object Executor {
  def main(args: Array[String]): Unit = {
    //启动服务器，来接受数据
    val server = new ServerSocket(9999)
    println("服务器启动，等待接收数据")

    //等待客户端的链接
    val clientLink: Socket = server.accept()

    val in: InputStream = clientLink.getInputStream
    val objIn: ObjectInputStream = new ObjectInputStream(in)
    val task: SubTask = objIn.readObject().asInstanceOf[SubTask]
    val ints: List[Int] = task.compute()
    println("[9999]计算节点计算的结果为" + ints)

    in.close()
    clientLink.close()
    server.close()


  }

}
