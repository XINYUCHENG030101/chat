 //生成唯一id的接口
 //对文件进行读写的接口

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <atomic>
#include <random>
#include <iomanip>
#include "logger.hpp"
 namespace yu
 {
    std::string uuid()
    {
        //生成6个0~255之间的随机数转化为12位16进制字符
        //通过静态变量生成一个2字节的编号数字
        std::random_device rd;//生成一个随机数
        std::mt19937 generator (rd());
        std::uniform_int_distribution<int> distribution(0,255); //限定范围
        std::stringstream ss;
        for(int i = 0;i < 6;i++)
        {
            if(i == 2)
                ss<<"-";
            ss<<std::setw(2)<<std::setfill('0')<<std::hex<<distribution(generator);
        }
        ss<<"-";
        static std::atomic<short> rds;
        short tmp = rds.fetch_add(1);
        ss<<std::setw(4)<<std::setfill('0')<<std::hex<<tmp;
        return ss.str();
    }
    std::string vcode() 
    {
        std::random_device rd;//实例化设备随机数对象-用于生成设备随机数
        std::mt19937 generator(rd());//以设备随机数为种子，实例化伪随机数对象
        std::uniform_int_distribution<int> distribution(0,9); //限定数据范围

        std::stringstream ss;
        for (int i = 0; i < 4; i++) {
            ss << distribution(generator);
        }
        return ss.str();
    }
    bool readfile(const std::string &file_name,std::string & body)
    {
        std::fstream rs(file_name,std::ifstream::binary | std::ifstream::in);
        if(rs.is_open() == false)
        {
            LOG_ERROR("文件打开失败{}",file_name);
            return false;
        }
        rs.seekg(0,rs.end);
        size_t lenth = rs.tellg();
        rs.seekg (0, rs.beg);
        body.resize(lenth);
        rs.read(&body[0],lenth);
        if(rs.good() == false)
        {
            LOG_ERROR("文件读取失败{}",file_name);
            rs.close();
            return false;
        }
        return true;
    }
    bool writefile(const std::string &file_name,const std::string & body)
    {
        //实现将body中的数据，写入filename对应的文件中
        std::ofstream ofs(file_name, std::ios::out | std::ios::binary | std::ios::trunc);
        if (ofs.is_open() == false) {
            LOG_ERROR("打开文件 {} 失败！", file_name);
            return false;
        }
        ofs.write(body.c_str(), body.size());
        if (ofs.good() == false) {
            LOG_ERROR("读取文件 {} 数据失败！", file_name);
            ofs.close();
            return false;
        }
        ofs.close();
        return true;
    }
 }