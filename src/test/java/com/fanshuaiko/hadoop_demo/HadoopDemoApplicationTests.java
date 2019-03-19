package com.fanshuaiko.hadoop_demo;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class HadoopDemoApplicationTests {

    @Autowired
    private HadoopController hadoopController;
    @Test
    public void contextLoads() {
    }


    @Test
    public void deleteTest(){
        String deletePath = "/test/testfile.txt";
        try {
            hadoopController.delete(deletePath,false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
