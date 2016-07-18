package meachinelearning.word2vec

import java.io.File

/**
  * Created by li on 16/7/15.
  */

object DeleteDirectory {

  /**
    * 删除空目录
    * @param dir 将要删除的目录路径
    */
  private def doDeleteEmptyDir(dir: String): Unit = {

    val success: Boolean = new File(dir).delete()

    if (success) {

      System.out.println("Successfully deleted empty directory: " + dir)

    } else {

      System.out.println("Failed to delete empty directory: " + dir)
    }
  }

  /**
    * 递归删除目录下的所有文件及子目录下所有文件
    * @param dir 将要删除的文件目录
    * @return boolean Returns "true" if all deletions were successful.
    *                 If a deletion fails, the method stops attempting to
    *                 delete and returns "false".
    */
  private def deleteDir(dir: File): Boolean = {

    if (dir.isDirectory) {

      val children = dir.list()

      //递归删除目录中的子目录下
      for (i <- 0 until children.length){

        val success = deleteDir(new File(dir, children(i)))

        if (! success){
          return false
        }

      }
    }
    // 目录此时为空，可以删除
    dir.delete()
  }


  /**
    *测试
    */
  def main(args: Array[String]): Unit = {

    val dir = "/Users/li/kunyan/DataSet/1111"

    doDeleteEmptyDir(dir)

    val  success = deleteDir(new File(dir))

    if (success) System.out.println("Successfully deleted populated directory: " + dir)

    else System.out.println("Failed to delete populated directory: " + dir)
  }

}

