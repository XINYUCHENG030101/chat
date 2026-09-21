#include<gflags/gflags.h>
#include<iostream>

DEFINE_bool(enable_debug,true,"是否开启调试模式");
DEFINE_int32(port,8080,"端口号");
DEFINE_string(ip,"127.0.0.1","ip地址");

int main(int argc,char* argv[])
{
    google::ParseCommandLineFlags(&argc,&argv,true);
    std::cout<<FLAGS_enable_debug<<std::endl;
    std::cout<<FLAGS_ip<<std::endl;
    std::cout<<FLAGS_port<<std::endl;


    return 0;
}