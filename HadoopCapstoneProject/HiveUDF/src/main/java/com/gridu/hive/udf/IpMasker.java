package com.gridu.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class IpMasker extends UDF {
    public IntWritable evaluate(Text ip, IntWritable networkSize) {
        IntWritable result = new IntWritable();
        result.set(Utilities.maskIp(ip.toString(), networkSize.get()));
        return result;
    }
}
