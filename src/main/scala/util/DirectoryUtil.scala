package util

import java.io.File

/**
  * Created by li on 16/7/18.
  * 文件夹处理工具,删除空文件夹, 删除非空文件夹及其中的文件
  */
object DirectoryUtil {

  /**
    * 删除空目录
    *
    * @param dir 将要删除的目录路径
    */
  def doDeleteEmptyDir(dir: String): Unit = {

    val success: Boolean = new File(dir).delete()

    if (success) {

      System.out.println("Successfully deleted empty directory: " + dir)

    } else {

      System.out.println("Failed to delete empty directory: " + dir)
    }
  }

  /**
    * 递归删除目录下的所有文件及子目录下所有文件
    *
    * @param dir 将要删除的文件目录
    * @return boolean Returns "true" if all deletions were successful.
    *                 If a deletion fails, the method stops attempting to
    *                 delete and returns "false".
    */
  def deleteDir(dir: File): Boolean = {

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

}
