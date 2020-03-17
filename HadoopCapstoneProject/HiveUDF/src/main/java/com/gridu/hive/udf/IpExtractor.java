package com.gridu.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class IpExtractor extends UDF  {
    public IntWritable evaluate(Text ip) {
        IntWritable result = new IntWritable();
        result.set(Utilities.extractIp(ip.toString()));
        return result;
    }
}
