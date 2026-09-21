
#include<gflags/gflags.h>
#include"file_server.hpp"
DEFINE_bool(enable_debug,false,"是否开启调试模式");
DEFINE_string(log_file,"","输出日志文件名");
DEFINE_int32(log_level,0,"发布模式下指定日志等级");

DEFINE_string(etcd_host,"http://127.0.0.1:2379","服务注册中心");
DEFINE_string(basename_service,"/service","服务器监控目录");
DEFINE_string(instance_service,"/file_service/instance","当前实例名称");
DEFINE_string(access_host,"http://127.0.0.1:10002","外部访问地址");
DEFINE_string(storage_path, "./data/", "当前实例的外部访问地址");
DEFINE_int32(listen_port, 10002, "Rpc服务器监听端口");
DEFINE_int32(rpc_timeout, -1, "Rpc调用超时时间");
DEFINE_int32(rpc_threads, 1, "Rpc的IO线程数量");


int main(int argc, char *argv[])
{
    google::ParseCommandLineFlags(&argc,&argv,true);    
    yu::init_logger(FLAGS_enable_debug,FLAGS_log_file,FLAGS_log_level);

    yu::FileServerBuilder fsb;
    fsb.make_reg(FLAGS_etcd_host,FLAGS_basename_service+FLAGS_instance_service,FLAGS_access_host);
    fsb.make_server(FLAGS_listen_port,FLAGS_rpc_timeout,FLAGS_rpc_threads,FLAGS_storage_path);
    auto server = fsb.build();
    server->start();
    return 0;
}