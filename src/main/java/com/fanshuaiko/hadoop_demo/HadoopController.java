package com.fanshuaiko.hadoop_demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.web.bind.annotation.*;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * @ClassName HadoopController
 * @Description Hadoop的hdfs的一些简单操作, Hadoop界面：http://localhost:50070
 * @Author fanshuaiko
 * @Date 19-3-18 下午5:30
 * @Version 1.0
 **/
@RestController
@RequestMapping("/HadoopController")
public class HadoopController {


    private Configuration conf;
    private FileSystem fs;

    public void connection() {
        //设置远程登录的用户（即服务器上操作hadoop的用户），默认是当前机器的用户，如果不设置会报拒绝登录的错误
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("root");
        try {
            ugi.doAs((PrivilegedExceptionAction) () -> {
                try {
                    //操作HA集群有两种方式，1.通过conf一个个的设置属性，2.将hdfs的两个配置文件放到resource目录下，new Configuration()的时候会自动读取，这种方法最简单
                    conf = new Configuration();
                    //设置集群别名，而不是具体的地址，避免硬编码，它会自动的选择active节点进行操作
                    conf.set("fs.defaultFS", "hdfs://mycluster");
                    conf.set("hadoop.job.ugi", "root");
                     fs = FileSystem.get(conf);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() throws IOException {

        fs.close();
    }

    //创建文件夹
    @PostMapping("/mkdir")
    public String mkdir(String mkdirPath) throws Exception {
        connection();
        boolean b = fs.mkdirs(new Path(mkdirPath));
        close();
        return "创建:" + b;
    }

    //下载文件
    @GetMapping("/downloadFile")
    public void downloadFile(String inPath, String outPath, int buffSize) throws Exception {
        connection();
        FSDataInputStream in = fs.open(new Path(inPath));
        FileOutputStream out = new FileOutputStream(outPath);
        IOUtils.copyBytes(in, out, buffSize, true);
        close();
        System.out.println("下载完成！");
    }

    //上传文件
    @PostMapping("/uploadFile")
    public void uploadFile(String inPath, String outPath, int buffSize) throws Exception {
        connection();
        FileInputStream in = new FileInputStream(inPath);
        FSDataOutputStream out = fs.create(new Path(outPath));
        IOUtils.copyBytes(in, out, buffSize, true);
        close();
        System.out.println("上传完成！");
    }

    //删除文件
    @PostMapping("/delete")
    public String delete(String deletePath, boolean recursive) throws Exception {
        connection();
        //递归删除,如果是文件夹,并且文件夹中有文件的话就填写true,否则填false
        boolean b = fs.delete(new Path(deletePath), recursive);
        close();
        return "删除" + deletePath + " " + b;
    }


}
