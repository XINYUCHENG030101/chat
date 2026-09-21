#include "speech.hpp"
#include<gflags/gflags.h>

DEFINE_bool(enable_debug,false,"是否开启调试模式");
DEFINE_string(log_file,"","输出日志文件名");
DEFINE_int32(log_level,0,"发布模式下指定日志等级");

DEFINE_string(etcd_host,"http://127.0.0.1:2379","服务注册中心");
DEFINE_string(basename_service,"/service","服务器监控目录");
DEFINE_string(instance_service,"/speech_service/instance","当前实例名称");
DEFINE_string(access_host,"http://127.0.0.1:10001","外部访问地址");

DEFINE_int32(listen_port, 10001, "Rpc服务器监听端口");
DEFINE_int32(rpc_timeout, -1, "Rpc调用超时时间");
DEFINE_int32(rpc_threads, 1, "Rpc的IO线程数量");

DEFINE_string(app_id, "", "语音平台应用ID");
DEFINE_string(api_key, "", "语音平台API密钥");
DEFINE_string(secret_key, "", "语音平台加密密钥");


int main(int argc, char *argv[])
{
    google::ParseCommandLineFlags(&argc,&argv,true);    
    yu::init_logger(FLAGS_enable_debug,FLAGS_log_file,FLAGS_log_level);

    yu::SpeechServerBuilder ssb;
    ssb.make_asr(FLAGS_app_id,FLAGS_api_key,FLAGS_secret_key);
    ssb.make_reg(FLAGS_etcd_host,FLAGS_basename_service+FLAGS_instance_service,FLAGS_access_host);
    ssb.make_server(FLAGS_listen_port,FLAGS_rpc_timeout,FLAGS_rpc_threads);
    auto server = ssb.build();
    server->start();
    return 0;
}