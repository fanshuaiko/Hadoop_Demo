package com.fanshuaiko.hadoop_demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @ClassName HadoopController
 * @Description Hadoop的hdfs的一些简单操作,Hadoop界面：http://localhost:50070
 * @Author fanshuaiko
 * @Date 19-3-18 下午5:30
 * @Version 1.0
 **/
@RestController
@RequestMapping("/HadoopController")
public class HadoopController {

    @Value("${hdfs.url}")
    private String hdfsURL; //hdfs路径
    @Value("${hdfs.user}")
    private String hdfsUser; //hdfs用户

    //创建文件夹
    @PostMapping("/mkdir")
    public String mkdir(String mkdirPath) throws Exception {
        FileSystem fs = FileSystem.get(new URI(hdfsURL), new Configuration(), hdfsUser);
        boolean b = fs.mkdirs(new Path(mkdirPath));
        return "创建"+b;
    }

    //下载文件
    @GetMapping("/downloadFile")
    public void downloadFile(String inPath,String outPath,int buffSize) throws Exception {
        FileSystem fs = FileSystem.get(new URI(hdfsURL), new Configuration(), hdfsUser);
        FSDataInputStream in = fs.open(new Path(inPath));
        FileOutputStream out = new FileOutputStream(outPath);
        IOUtils.copyBytes(in, out, buffSize, true);
        System.out.println("下载完成！");
    }

    //上传文件
    @PostMapping("/uploadFile")
    public void uploadFile(String inPath,String outPath,int buffSize) throws Exception {
        FileSystem fs = FileSystem.get(new URI(hdfsURL), new Configuration(), hdfsUser);
        FileInputStream in = new FileInputStream(inPath);
        FSDataOutputStream out = fs.create(new Path(outPath));
        IOUtils.copyBytes(in,out,buffSize,true);
        System.out.println("上传完成！");
    }
    //删除文件
    @PostMapping("/delete")
    public String delete(String deletePath,boolean recursive) throws Exception{
        FileSystem fs = FileSystem.get(new URI(hdfsURL), new Configuration(), hdfsUser);
        //递归删除,如果是文件夹,并且文件夹中有文件的话就填写true,否则填false
        boolean b = fs.delete(new Path(deletePath), recursive);
        return "删除"+deletePath+" "+b;
    }


}
