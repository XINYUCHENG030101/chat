#include<gflags/gflags.h>
#include"message_server.hpp"
DEFINE_bool(enable_debug,false,"是否开启调试模式");
DEFINE_string(log_file,"","输出日志文件名");
DEFINE_int32(log_level,0,"发布模式下指定日志等级");

DEFINE_string(etcd_host,"http://127.0.0.1:2379","服务注册中心");
DEFINE_string(basename_service,"/service","服务器监控目录");
DEFINE_string(user_service, "/service/user_service", "用户管理子服务名称");
DEFINE_string(file_service, "/service/file_service", "文件管理子服务名称");
DEFINE_string(instance_service,"/message_service/instance","当前实例名称");
DEFINE_string(access_host,"http://127.0.0.1:10005","外部访问地址");



DEFINE_string(mysql_host, "127.0.0.1", "Mysql服务器访问地址");
DEFINE_string(mysql_user, "root", "Mysql服务器访问用户名");
DEFINE_string(mysql_pswd, "123456", "Mysql服务器访问密码");
DEFINE_string(mysql_db, "yu_chat", "Mysql默认库名称");
DEFINE_string(mysql_cset, "utf8", "Mysql客户端字符集");
DEFINE_int32(mysql_port, 0, "Mysql服务器访问端口");
DEFINE_int32(mysql_pool_count, 4, "Mysql连接池最大连接数量");


DEFINE_bool(redis_keep_alive, true, "Redis长连接保活选项");
DEFINE_int32(listen_port, 10005, "Rpc服务器监听端口");
DEFINE_int32(rpc_timeout, -1, "Rpc调用超时时间");
DEFINE_int32(rpc_threads, 1, "Rpc的IO线程数量");

DEFINE_string(mq_user, "root", "消息队列用户名");
DEFINE_string(mq_passwd, "123456", "消息队列密码");
DEFINE_string(mq_host, "127.0.0.1:5672", "消息队列地址");
DEFINE_string(mq_msg_exchange, "msg_exchange", "持久化消息的发布交换机名称");
DEFINE_string(mq_msg_queue, "msg_queue", "持久化消息的发布队列名称");
DEFINE_string(mq_msg_binding_key, "msg_queue", "持久化消息的发布队列名称");

int main(int argc, char *argv[])
{
    google::ParseCommandLineFlags(&argc,&argv,true);    
    yu::init_logger(FLAGS_enable_debug,FLAGS_log_file,FLAGS_log_level);

    yu::MessageServerBuilder fsb;
    fsb.make_mq_client(FLAGS_mq_user,FLAGS_mq_passwd,FLAGS_mq_host,FLAGS_mq_msg_exchange,FLAGS_mq_msg_queue,FLAGS_mq_msg_binding_key);
    fsb.make_db_client(FLAGS_mysql_user,FLAGS_mysql_pswd,FLAGS_mysql_db,FLAGS_mysql_host,
        FLAGS_mysql_cset,FLAGS_mysql_port,FLAGS_mysql_pool_count);
    fsb.make_discovery_client(FLAGS_etcd_host,FLAGS_basename_service,FLAGS_file_service, FLAGS_user_service);
    fsb.make_reg(FLAGS_etcd_host,FLAGS_basename_service+FLAGS_instance_service,FLAGS_access_host);
    fsb.make_server(FLAGS_listen_port,FLAGS_rpc_timeout,FLAGS_rpc_threads);
    auto server = fsb.build();
    server->start();
    return 0;
}