#include"../common/etcd.hpp"
#include<gflags/gflags.h>
#include"../common/logger.hpp"
#include<thread>

DEFINE_bool(enable_debug,true,"是否开启调试模式");
DEFINE_string(log_file,"","输出日志文件名");
DEFINE_int32(log_level,0,"发布模式下指定日志等级");

DEFINE_string(etcd_host,"http://127.0.0.1:2379","服务注册中心");
DEFINE_string(basename_service,"/service","服务器监控目录");
DEFINE_string(instance_service,"/friend/instance","当前实例名称");


int main(int argc,char* argv[])
{
    google::ParseCommandLineFlags(&argc,&argv,true);
    
    yu::init_logger(FLAGS_enable_debug,FLAGS_log_file,FLAGS_log_level);
    yu::Registry::ptr rclient = std::make_shared<yu::Registry>(FLAGS_etcd_host);
    
    rclient->registry(FLAGS_basename_service+FLAGS_instance_service,FLAGS_etcd_host);
    std::this_thread::sleep_for(std::chrono::seconds(600));

    return 0;
}