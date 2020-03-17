package com.gridu.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class NetworkSizeExtractor extends UDF  {
    public IntWritable evaluate(Text ip) {
        IntWritable result = new IntWritable();
        result.set(Utilities.getNetworkSize(ip.toString()));
        return result;
    }
}
