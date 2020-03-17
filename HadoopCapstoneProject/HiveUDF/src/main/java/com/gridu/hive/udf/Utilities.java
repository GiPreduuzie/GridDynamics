package com.gridu.hive.udf;

public class Utilities {

    static public int extractIp(String ip) {
        return ipToInt(ip.split("/")[0]);
    }

    public static int getNetworkSize(String ip) {
        return Integer.parseInt(ip.split("/")[1]);
    }

    static public int maskIp(String ip, int networkSize) {
        if (networkSize == 0)
            return 0;
        int emptyBits = 32 - networkSize;
        return -1 >>> emptyBits << emptyBits & ipToInt(ip);
    }

    static private int ipToInt(String ip) {
        String[] parts = ip.split("\\.");
        int acc = 0;
        int i = 0;
        while (i < parts.length) {
            int part = Integer.parseInt(parts[i]);
            if (part < 0 || part > 255) throw new IllegalArgumentException("ip {" + ip + "} is malformed");
            acc = (acc << 8) + Integer.parseInt(parts[i]);
            i++;
        }
        return acc;
    }

}
