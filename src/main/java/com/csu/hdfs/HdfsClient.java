package com.csu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @ClassName: HdfsClient
 * @Description:  api operations
 * @Author: Achilles
 * @Date: 09/09/2019  18:06
 * @Version: 1.0
 **/

public class HdfsClient {

    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException{

        HdfsClient hc = new HdfsClient();
        FileSystem fs = hc.getFileSystem();

//        //1.测试新建HDFS目录
//        hc.testMkdir(fs);

//        //2.测试从本地上传文件到HDFS
//        hc.testCopyFromLocalFile(fs);

//        //3.测试从HDFS下载文件到本地
//        hc.testCopyToLocalFile(fs);
//        //todo
//        //如果写在C盘会报错

//        //4.测试从HDFS下载文件到本地，不删除源文件，使用本地文件系统
//        hc.testCopyToLocalFileWhethterDeleting(fs,false,true);
//        //todo 此参数生成后无crc校验文件

//        //5.测试删除文件
//        //boolean是否循环删除
//        hc.testDelete(fs,true);

//        //6.测试重命名文件
//        hc.testRename(fs);

//        //7.测试文件详情查看
//        hc.testListFiles(fs,new Path("/"));

//        //8.文件和文件夹判断,即列出所给目录下的文件和文件夹
//        hc.testListStatus(fs,new Path("/"));


    }

    public void printInfo(){

        System.out.println("Hadoop操作成功完成！！");
    }

    public FileSystem getFileSystem() throws URISyntaxException,IOException,InterruptedException {

        return FileSystem.get(new URI("hdfs://master:9000"),new Configuration(),"csu");

    }

    public void testMkdir(FileSystem fs) throws IOException {
        fs.mkdirs(new Path("/test/"));
        fs.close();
        printInfo();
    }

    public void testCopyFromLocalFile(FileSystem fs) throws IOException{
        fs.copyFromLocalFile(new Path("D:\\hadoop-2.7.2\\LICENSE.txt"),new Path("/test/forTest"));
        fs.close();
        printInfo();

    }

    public void testCopyToLocalFile(FileSystem fs) throws IOException {
        fs.copyToLocalFile(new Path("/test/testForReplicationInCode.txt"),
                new Path("D:\\testDownload.txt"));
        fs.close();
        printInfo();
    }

    public void testCopyToLocalFileWhethterDeleting(FileSystem fs,boolean flagDelSrc,
                                                    boolean flagUseRawLocalFS) throws IOException{
        fs.copyToLocalFile(flagDelSrc,new Path("/test/testForReplicationInCode.txt"),
                new Path("D:\\testDownload.txt"),flagUseRawLocalFS);
        fs.close();
        printInfo();

    }

    public void testDelete(FileSystem fs,boolean recuresive) throws IOException {
        fs.delete(new Path("/test"),recuresive);
        //是否循环删除
        //fs.listFiles(new Path("/"),true);
        fs.close();
        printInfo();
    }

    public void testRename(FileSystem fs) throws IOException {

//        //新建目录
//        fs.mkdirs(new Path("/test"));
//        //上传新文件
//        fs.copyFromLocalFile(new Path("D:\\hadoop-2.7.2\\README.txt"),new Path("/test/testRename.txt"));

//        fs.delete(new Path("/user"),true);
//        fs.copyFromLocalFile(new Path("D:\\hadoop-2.7.2\\README.txt"),new Path("/testForMkdir.txt"));

        //执行改名操作
        fs.rename(new Path("/test/testRename.txt"),new Path("/test/renameDone.txt"));
        fs.rename(new Path("/testForMkdir.txt"),new Path("/renameDone.txt"));

        //查看是否改名成功
        testListFiles(fs,new Path("/"));
        //关闭文件资源
        fs.close();
        printInfo();

    }

    public void testListFiles(FileSystem fs,Path fp) throws IOException {

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(fp,true);

        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            //查看文件名称，权限，长度，块信息
            System.out.println(fileStatus.getPath());//文件路径
            System.out.println(fileStatus.getPath().getName());//文件名称
            System.out.println(fileStatus.getPermission());//文件权限
            System.out.println(fileStatus.getLen());//文件长度

            BlockLocation[] blockLocations = fileStatus.getBlockLocations();//存储块
            for (BlockLocation blockLocation : blockLocations) {

                String[] hosts = blockLocation.getHosts();

                for (String host:hosts) {
                    System.out.println(host);
                }
            }

            System.out.println("-----------分割线-----------");
        }
        fs.close();
        printInfo();
    }

    public void testListStatus(FileSystem fs,Path path) throws IOException {

        FileStatus[] listStatus = fs.listStatus(path);
        for (FileStatus fileStatus:listStatus) {
            if(fileStatus.isFile()) {//文件
                System.out.println("f:"+fileStatus.getPath().getName());
            }else {//文件夹
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }
        fs.close();
        printInfo();
    }

}