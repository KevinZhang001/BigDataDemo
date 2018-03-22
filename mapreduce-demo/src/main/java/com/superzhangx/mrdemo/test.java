package com.superzhangx.mrdemo;

/**
 * Created by Administrator on 2018/3/22.
 */
public class test {
    public static void main(String[] args) {
        //System.out.println(stringToUnicode("﻿1"));
        System.out.println(1/10);
    }

    //字符串转换unicode
    public static String stringToUnicode(String string) {
        StringBuffer unicode = new StringBuffer();
        for (int i = 0; i < string.length(); i++) {
            char c = string.charAt(i);  // 取出每一个字符
            unicode.append("\\u" +Integer.toHexString(c));// 转换为unicode
        }
        return unicode.toString();
    }
}
