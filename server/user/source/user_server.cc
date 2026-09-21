
#include<gflags/gflags.h>
#include"user_server.hpp"
DEFINE_bool(enable_debug,false,"是否开启调试模式");
DEFINE_string(log_file,"","输出日志文件名");
DEFINE_int32(log_level,0,"发布模式下指定日志等级");

DEFINE_string(etcd_host,"http://127.0.0.1:2379","服务注册中心");
DEFINE_string(basename_service,"/service","服务器监控目录");

DEFINE_string(instance_service,"/user_service/instance","当前实例名称");
DEFINE_string(access_host,"http://127.0.0.1:10003","外部访问地址");
DEFINE_string(file_service,"/service/file_service","文件管理子服务名称");

DEFINE_string(es_host, "http://127.0.0.1:9200/", "ES搜索引擎服务器URL");

DEFINE_string(mysql_host, "127.0.0.1", "Mysql服务器访问地址");
DEFINE_string(mysql_user, "root", "Mysql服务器访问用户名");
DEFINE_string(mysql_pswd, "123456", "Mysql服务器访问密码");
DEFINE_string(mysql_db, "yu_chat", "Mysql默认库名称");
DEFINE_string(mysql_cset, "utf8", "Mysql客户端字符集");
DEFINE_int32(mysql_port, 0, "Mysql服务器访问端口");
DEFINE_int32(mysql_pool_count, 4, "Mysql连接池最大连接数量");


DEFINE_string(redis_host, "127.0.0.1", "Redis服务器访问地址");
DEFINE_int32(redis_port, 6379, "Redis服务器访问端口");
DEFINE_int32(redis_db, 0, "Redis默认库号");
DEFINE_bool(redis_keep_alive, true, "Redis长连接保活选项");
DEFINE_int32(listen_port, 10003, "Rpc服务器监听端口");
DEFINE_int32(rpc_timeout, -1, "Rpc调用超时时间");
DEFINE_int32(rpc_threads, 1, "Rpc的IO线程数量");


int main(int argc, char *argv[])
{
    google::ParseCommandLineFlags(&argc,&argv,true);    
    yu::init_logger(FLAGS_enable_debug,FLAGS_log_file,FLAGS_log_level);

    yu::UserServerBuilder fsb;
    fsb.make_es_client({FLAGS_es_host});
    fsb.make_db_client(FLAGS_mysql_user,FLAGS_mysql_pswd,FLAGS_mysql_db,FLAGS_mysql_host,
        FLAGS_mysql_cset,FLAGS_mysql_port,FLAGS_mysql_pool_count);
    fsb.make_redis_client(FLAGS_redis_host,FLAGS_redis_port,
        FLAGS_redis_db,FLAGS_redis_keep_alive);
    fsb.make_sender();
    fsb.make_discovery_client(FLAGS_etcd_host,FLAGS_basename_service,FLAGS_file_service);
    fsb.make_reg(FLAGS_etcd_host,FLAGS_basename_service+FLAGS_instance_service,FLAGS_access_host);
    fsb.make_server(FLAGS_listen_port,FLAGS_rpc_timeout,FLAGS_rpc_threads);
    auto server = fsb.build();
    server->start();
    return 0;
}