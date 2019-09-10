package com.csu.hdfs;

import com.jcraft.jsch.IO;
import com.sun.corba.se.spi.ior.IORTemplate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @ClassName: HDFSIO
 * @Description: io operation in stream
 * @Author: Achilles
 * @Date: 10/09/2019  14:57
 * @Version: 1.0
 **/

public class HDFSIO {

    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {

        HDFSIO hdfsio = new HDFSIO();
//        hdfsio.putFileToHDFS();
//        hdfsio.listFiles();
//        hdfsio.getFileFromHDFS();
        hdfsio.readFileSeek1();
        hdfsio.readFileSeek2();
    }

    //把本地D盘上的某文件上传到HDFS根目录
    public void putFileToHDFS() throws URISyntaxException, IOException, InterruptedException {

        //1 获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), conf, "csu");


        //2 获取输入流
        FileInputStream fis = new FileInputStream(new File("D:\\hadoop-2.7.2\\LICENSE.txt"));

        //3 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/LICENSE.txt"));

        //4 流的对转
        IOUtils.copyBytes(fis,fos,conf);

        //5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();

    }

    //将HDFS上的文件下载到本地
    public void getFileFromHDFS() throws URISyntaxException, IOException, InterruptedException {
        //1 获取文件系统对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), conf, "csu");

        //2 获取输入流(输入流是HDFS)
        FSDataInputStream fis = fs.open(new Path("/LICENSE.txt"));

        //3 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("D:\\LICENSE.txt"));

        //4 对接流
        IOUtils.copyBytes(fis, fos, conf);

        //5 关闭流
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    public void listFiles() throws URISyntaxException, IOException, InterruptedException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"),conf,"csu");

        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator =
                fs.listFiles(new Path("/"), true);
        while (locatedFileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus locatedFileStatus = locatedFileStatusRemoteIterator.next();

            System.out.println(locatedFileStatus.getPath());
            System.out.println(locatedFileStatus.getPath().getName());
            System.out.println(locatedFileStatus.getLen());
            System.out.println(locatedFileStatus.getPermission());

            BlockLocation[] blockLocations = locatedFileStatus.getBlockLocations();
            for(BlockLocation blockLocation:blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for(String host:hosts) {
                    System.out.println(host);
                }
            }

            System.out.println("------------分割线------------");
        }
        fs.close();

    }

    //  下载第一块
    public void readFileSeek1() throws URISyntaxException, IOException, InterruptedException {

        //1 获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"),conf,"csu");

//        // 将大文件上传到HDFS
//        System.out.println("开始上传文件");
//        fs.copyFromLocalFile(new Path("G:\\4读研学习资料\\大数据\\jdk-8u221-linux-x64.tar.gz"),
//                new Path("/test/bigFiles/jdk-8u221-linux-x64.tar.gz"));
//        System.out.println("文件上传完成");
        //2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/test/bigFiles/jdk-8u221-linux-x64.tar.gz"));

        //3 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("D:\\jdk-8u221-linux-x64.tar.gz.part1"));

        //4 输入输出流对接，只拷取128M
//        IOUtils.copyBytes(fis,fos,conf);
        byte[] buffer = new byte[1024];
        for(int i = 0;i < 1024 * 128;i++){//128M
            fis.read(buffer);
            fos.write(buffer);
        }

        //5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
        System.out.println("Hadoop全部操作完成！！");
    }

    public void readFileSeek2() throws URISyntaxException, IOException, InterruptedException {
        //1 获取文件对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), conf, "csu");

        //2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/test/bigFiles/jdk-8u221-linux-x64.tar.gz"));

        //3 设置指定读取的起点
        fis.seek(1024*1024*128);

        //4 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("D:\\jdk-8u221-linux-x64.tar.gz.part2"));

        //5 流的对拷
        IOUtils.copyBytes(fis,fos,conf);

        //6 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
        System.out.println("Hadoop全部操作完成！！");
    }


}
