#pragma once

#include <brpc/server.h>
#include <butil/logging.h>

#include "etcd.hpp"     // 服务注册模块封装
#include "logger.hpp"   // 日志模块封装
#include "rabbitmq.hpp"
#include "channel.hpp"
#include "utils.hpp"
#include "mysql_session_member.hpp"

#include "base.pb.h"  // protobuf框架代码
#include "usr.pb.h"  // protobuf框架代码
#include "transmit.pb.h"  // protobuf框架代码

namespace yu
{
    class TransmitServiceIpml:public yu::MsgTransmitService
    {
    public:
        TransmitServiceIpml(const std::string& user_service_name,
            const ServiceManager::ptr& mm_channels,
            const std::shared_ptr<odb::core::database>& db,
            const std::string& exchange_name,
            const std::string& routing_key,
            const MQClient::ptr& mq_client)
        :_user_service_name(user_service_name)
        ,_mm_channels(mm_channels)
        ,_mysql_session_member_table(std::make_shared<ChatSessionMemberTable>(db))
        ,_exchange_name(exchange_name)
        ,_routing_key(routing_key)
        ,_mq_client(mq_client)
        {}
        ~TransmitServiceIpml(){}
        void GetTransmitTarget(google::protobuf::RpcController* controller,
                       const ::yu::NewMessageReq* req,
                       ::yu::GetTransmitTargetRsp* rsp,
                       ::google::protobuf::Closure* done) 
        {
            brpc::ClosureGuard rpc_guard(done);
            auto err_ret = [this,rsp](const std::string &rid,
                                            const std::string &errmsg)
            {
                rsp->set_request_id(rid);
                rsp->set_success(false);
                rsp->set_errmsg(errmsg);
                return;
            };
            //1. 从请求中取出消息内容，会话ID， 用户ID
            std::string rid = req->request_id();
            std::string chat_ssid = req->chat_session_id();
            std::string uid = req->user_id();
            const MessageContent& content = req->message();
            //2. 根据用户ID 从用户子服务获取当前发送者用户信息
            auto channel = _mm_channels->choose(_user_service_name);
            if(!channel)
            {
                LOG_ERROR("未找到用户子服务节点{}-{}",rid,chat_ssid);
                return err_ret(rid,"未找到用户子服务节点");
            }
            //3. 根据消息内容构造完成的消息结构（分配消息ID，填充发送者信息，填充消息产生时间）
            yu::UserService_Stub stub(channel.get());
            yu::GetUserInfoReq request;
            yu::GetUserInfoRsp response;
            request.set_request_id(rid);
            request.set_user_id(uid);
            brpc::Controller cntl;
            stub.GetUserInfo(&cntl,&request,&response,nullptr);
            if(cntl.Failed() == true||response.success() == false)
            {
                LOG_ERROR("用户子服务调用失败{}-{}",request.request_id(),cntl.ErrorText());
                err_ret(request.request_id(),"用户子服务调用失败");
                return;
            }
            MessageInfo message;
            message.set_message_id(uuid());
            message.set_chat_session_id(chat_ssid);
            message.set_timestamp(time(nullptr));
            message.mutable_sender()->CopyFrom(response.user_info());
            message.mutable_message()->CopyFrom(content);
            //4. 将消息序列化后发布到MQ 消息队列中，让消息存储子服务对消息进行持久化存储
            bool ret = _mq_client->publish(_exchange_name,message.SerializeAsString(),_routing_key);
            if(ret == false)
            {
                LOG_ERROR("持久化消息发布失败{}",rid);
                return err_ret(rid,"持久化消息发布失败");
            }
            //5. 从数据库获取目标会话所有成员ID
            auto target_list = _mysql_session_member_table->get_user_id(chat_ssid);
            //6. 组织响应（完整消息+目标用户ID），发送给网关，告知网关该将消息发送给谁。
            rsp->set_request_id(rid);
            rsp->set_success(true);
            rsp->mutable_message()->CopyFrom(message);
            for(const auto& list:target_list)
            {
                rsp->add_target_id_list(list);
            }
        }
    private:
        //用户子服务调用
        std::string _user_service_name;
        ServiceManager::ptr _mm_channels;
        //数据库
        ChatSessionMemberTable::ptr _mysql_session_member_table;
        //消息队列
        std::string _exchange_name;
        std::string _routing_key;
        MQClient::ptr _mq_client;

    };

    class  TransmitServer
    {
    public:
        using ptr = std::shared_ptr<TransmitServer>;
        TransmitServer( 
            const Discovery::ptr &service_discovery,
            const Registry::ptr &reg_client,
            const std::shared_ptr<odb::core::database>& mysql_client,
            const std::shared_ptr<brpc::Server> &server)
        :_service_discover(service_discovery)
        ,_reg_client(reg_client)
        ,_mysql_client(mysql_client)
        ,_rpc_server(server)
        {}
        ~TransmitServer(){}
        //启动服务
        void start()
        {
            _rpc_server->RunUntilAskedToQuit();
        }
    private:
        Discovery::ptr _service_discover;
        Registry::ptr _reg_client;
        std::shared_ptr<odb::core::database> _mysql_client;
        std::shared_ptr<brpc::Server> _rpc_server;
    };
    class  TransmitServerBuilder
    {
    public:
         void make_db_client( const std::string &user,
                const std::string &pswd,
                const std::string &db,
                const std::string &host,
                const std::string &cset,
                int port,
                int conn_pool_count)
        {
            _mysql_client = ODBFactory::create(user,pswd,db,host,cset,port,conn_pool_count);
        }
        void make_discovery_client(const std::string &host,
                                    const std::string &base_service_name,
                                    const std::string &user_service_name)
        {
            _mm_channels = std::make_shared<ServiceManager>();
            _mm_channels->declared(user_service_name);
            auto put_cb = std::bind(&ServiceManager::onServiceOnline, _mm_channels.get(), std::placeholders::_1, std::placeholders::_2);
            auto del_cb = std::bind(&ServiceManager::onServiceOffline, _mm_channels.get(), std::placeholders::_1, std::placeholders::_2);
            _service_discover = std::make_shared<Discovery>(host,base_service_name,put_cb,del_cb);
        }
         //用于构造服务注册客户端对象
        void make_reg(const std::string& etcd_host ,const std::string& service_name,const std::string& access_host)
        {
            _reg_client = std::make_shared<Registry>(etcd_host);
            _reg_client->registry(service_name,access_host);
        }
        //构造rabbitmq服务器对象
        void make_mq_client(const std::string &user, 
            const std::string& passwd,
            const std::string& host,
            const std::string& exchange,
            const std::string& queue_name,
            const std::string& binding_key)
        {
            _routing_key = binding_key;
            _exchange_name = exchange;
            _mq_client = std::make_shared<MQClient>(user,passwd,host);
            _mq_client->declareComponents(exchange,queue_name,binding_key);
        }
        //构造RPC服务器对象
        void make_server(uint16_t port, int32_t timeout, uint8_t num_threads)
        {
            if (!_mq_client) {
                LOG_ERROR("还未初始化消息队列客户端模块！");
                abort();
            }
            if (!_mm_channels) {
                LOG_ERROR("还未初始化信道管理模块！");
                abort();
            }
            if(!_mysql_client)
            {
                LOG_ERROR("mysql客户端未初始化");
                abort();
            }
            _rpc_server = std::make_shared<brpc::Server>();
            TransmitServiceIpml *transmit_service = new TransmitServiceIpml(_user_service_name
                ,_mm_channels
                ,_mysql_client,_exchange_name,_routing_key,_mq_client);
            int ret = _rpc_server->AddService(transmit_service, brpc::ServiceOwnership::SERVER_DOESNT_OWN_SERVICE);
            if (ret == -1) 
            {
                LOG_ERROR("添加RPC服务器失败");
                abort();
            }
            _rpc_server = std::make_shared<brpc::Server>();
            brpc::ServerOptions options;
            options.idle_timeout_sec = timeout; //连接空闲超时时间-超时后连接被关闭
            options.num_threads = num_threads; // io线程数量
            ret = _rpc_server->Start(port, &options);
            if (ret == -1) {
                LOG_ERROR("服务启动失败！");
                abort();
            }

        }
        TransmitServer::ptr build()
        {
            if(!_service_discover)
            {
                LOG_ERROR("服务发现模块创建失败");
                abort();
            }
            if(!_reg_client)
            {
                LOG_ERROR("注册服务创建失败");
                abort();
            }
            if(!_rpc_server)
            {
                LOG_ERROR("服务器创建失败");
                abort();
            }
            TransmitServer::ptr server = std::make_shared<TransmitServer>( _service_discover,_reg_client,_mysql_client,_rpc_server);
            return server;
        }

    private:
        std::string _user_service_name;
        ServiceManager::ptr _mm_channels;
        Discovery::ptr _service_discover;

        std::string _routing_key;
        std::string _exchange_name;
        MQClient::ptr _mq_client;

        Registry::ptr _reg_client;
        std::shared_ptr<odb::core::database> _mysql_client; 
        std::shared_ptr<brpc::Server> _rpc_server;
    };
}
