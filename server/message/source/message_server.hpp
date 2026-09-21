#pragma once
#include <brpc/server.h>
#include <butil/logging.h>
#include "data_es.hpp"            // es数据管理客户端封装
#include "mysql_message.hpp"      // mysql数据管理客户端封装
#include "etcd.hpp"     // 服务注册模块封装
#include "logger.hpp"   // 日志模块封装
#include "utils.hpp"    // 基础工具接口
#include "channel.hpp"  // 信道管理模块封装
#include "rabbitmq.hpp"  // rabbitmq消息队列模块封装
#include "message.pb.h"  // protobuf框架代码
#include "base.pb.h"  // protobuf框架代码
#include "file.pb.h"  // protobuf框架代码
#include "usr.pb.h"  // protobuf框架代码
#include "data_redis.hpp"  // redis数据管理客户端封装
#include <unordered_set>
#include <unordered_map>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <string>
namespace yu
{
    class MessageServiceIpml:public yu::MsgStorageService
    {
    public:
        MessageServiceIpml(
            const std::shared_ptr<elasticlient::Client> &es_client,
            const std::shared_ptr<odb::core::database> &db,
            const ServiceManager::ptr &channel_manager,
            const std::string &file_service_name,
            const std::string &user_service_name
        )
        :_es_message(std::make_shared<ESMessage>(es_client)),  
        _mysql_message(std::make_shared<Messagetable>(db)),
        _file_service_name(file_service_name),
        _mm_channels(channel_manager),
        _user_service_name(user_service_name)
        {}
        ~MessageServiceIpml(){}
        virtual void GetHistoryMsg(google::protobuf::RpcController* controller,
                       const ::yu::GetHistoryMsgReq* request,
                       ::yu::GetHistoryMsgRsp* response,
                       ::google::protobuf::Closure* done)
            {
                brpc::ClosureGuard rpc_guard(done);
                auto err_ret = [this,response](const std::string &rid,
                                                const std::string &errmsg)
                {
                    response->set_request_id(rid);
                    response->set_success(false);
                    response->set_errmsg(errmsg);
                    return;
                };
                //1、提取会话id，以及起始时间
                std::string rid = request->request_id();
                std::string chat_ssid = request->chat_session_id();
                boost::posix_time::ptime stime = boost::posix_time::from_time_t(request->start_time());
                boost::posix_time::ptime etime = boost::posix_time::from_time_t(request->over_time());
                //2、从数据库中进行消息查询
                auto msg_lists = _mysql_message->range(chat_ssid,stime,etime);
                if(msg_lists.empty())
                {
                    response->set_request_id(rid);
                    response->set_success(true);
                    return;
                }
                //3、统计所有文件消息id，并从文件子服务中进行批量文件下载
                std::unordered_set<std::string> file_ids_lists;
                for(auto &msg:msg_lists)
                {
                    if(msg.file_id().empty())
                    {
                        continue;
                    }
                    file_ids_lists.insert(msg.file_id());
                }
                std::unordered_map<std::string,std::string> file_data_lists;
                bool ret = _getfile(rid,file_ids_lists,file_data_lists);
                if(!ret)
                {
                    LOG_ERROR("获取文件数据失败",rid);
                    return err_ret(rid,"获取文件数据失败");
                }
                //4、统计所有用户id，从用户子服务上进行批量获取
                std::unordered_set<std::string> user_ids_lists;
                for(auto &msg:msg_lists)
                {
                    if(msg.user_id().empty())
                    {
                        continue;
                    }
                    user_ids_lists.insert(msg.user_id());
                }
                std::unordered_map<std::string,UserInfo> user_lists;
                ret = _getuser(rid,user_ids_lists,user_lists);
                if(!ret)
                {
                    LOG_ERROR("获取用户数据失败",rid);
                    return err_ret(rid,"获取用户数据失败");
                }
                //5、构造响应
                response->set_request_id(rid);
                response->set_success(true);
                for(auto &msg:msg_lists)
                {
                    auto message_info = response->add_msg_list();
                    message_info->set_message_id(msg.message_id());
                    message_info->set_chat_session_id(msg.session_id());
                    message_info->set_timestamp(boost::posix_time::to_time_t(msg.create_time()));
                    message_info->mutable_sender()->CopyFrom(user_lists[msg.user_id()]);
                    switch(msg.message_type())
                    {
                        case MessageType::STRING:
                            message_info->mutable_message()->set_message_type(MessageType::STRING);
                            message_info->mutable_message()->mutable_string_message()->set_content(msg.content());
                            break;
                        case MessageType::IMAGE:
                            message_info->mutable_message()->set_message_type(MessageType::IMAGE);
                            message_info->mutable_message()->mutable_image_message()->set_file_id(msg.file_id());
                            message_info->mutable_message()->mutable_image_message()->set_image_content(file_data_lists[msg.file_id()]);
                            break;
                        case MessageType::FILE:
                            message_info->mutable_message()->set_message_type(MessageType::FILE);
                            message_info->mutable_message()->mutable_file_message()->set_file_id(msg.file_id());
                            message_info->mutable_message()->mutable_file_message()->set_file_size(msg.file_size());
                            message_info->mutable_message()->mutable_file_message()->set_file_name(msg.file_name());
                            message_info->mutable_message()->mutable_file_message()->set_file_contents(file_data_lists[msg.file_id()]);
                            break;
                        case MessageType::SPEECH:
                            message_info->mutable_message()->set_message_type(MessageType::SPEECH);
                            message_info->mutable_message()->mutable_speech_message()->set_file_id(msg.file_id());
                            message_info->mutable_message()->mutable_speech_message()->set_file_contents(file_data_lists[msg.file_id()]);
                            break;
                        default:
                            LOG_ERROR("未知的消息类型");
                            break;
                    }
                   
                }
            }
        virtual void GetRecentMsg(google::protobuf::RpcController* controller,
                            const ::yu::GetRecentMsgReq* request,
                            ::yu::GetRecentMsgRsp* response,
                            ::google::protobuf::Closure* done)
            {
                brpc::ClosureGuard rpc_guard(done);
                auto err_ret = [this,response](const std::string &rid,
                                                const std::string &errmsg)
                {
                    response->set_request_id(rid);
                    response->set_success(false);
                    response->set_errmsg(errmsg);
                    return;
                };
                //1、从请求中取出相关参数 请求id 会话id 消息条数
                std::string rid = request->request_id();
                std::string session_id = request->chat_session_id();
                int32_t msg_count = request->msg_count();
                //2、从数据库中获取消息元信息
                auto msg_lists = _mysql_message->recent(session_id,msg_count);
                if(msg_lists.empty())
                {
                    response->set_request_id(rid);
                    response->set_success(true);
                    return;
                }
                //3、组织消息id从文件子服务中进行文件下载
                std::unordered_set<std::string> file_ids_lists;
                for(auto &msg:msg_lists)
                {
                    if(msg.file_id().empty())
                    {
                        continue;
                    }
                    file_ids_lists.insert(msg.file_id());
                }
                std::unordered_map<std::string,std::string> file_data_lists;
                bool ret = _getfile(rid,file_ids_lists,file_data_lists);
                if(!ret)
                {
                    LOG_ERROR("获取文件数据失败",rid);
                    return err_ret(rid,"获取文件数据失败");
                }
                //4、组织用户id从用户子服务进行用户获取
                std::unordered_set<std::string> user_ids_lists;
                for(auto &msg:msg_lists)
                {
                    if(msg.user_id().empty())
                    {
                        continue;
                    }
                    user_ids_lists.insert(msg.user_id());
                }
                std::unordered_map<std::string,UserInfo> user_lists;
                ret = _getuser(rid,user_ids_lists,user_lists);
                if(!ret)
                {
                    LOG_ERROR("获取用户数据失败",rid);
                    return err_ret(rid,"获取用户数据失败");
                }
                //5、组织响应数据
                response->set_request_id(rid);
                response->set_success(true);
                for(auto &msg:msg_lists)
                {
                    auto message_info = response->add_msg_list();
                    message_info->set_message_id(msg.message_id());
                    message_info->set_chat_session_id(msg.session_id());
                    message_info->set_timestamp(boost::posix_time::to_time_t(msg.create_time()));
                    message_info->mutable_sender()->CopyFrom(user_lists[msg.user_id()]);
                    switch(msg.message_type())
                    {
                        case MessageType::STRING:
                            message_info->mutable_message()->set_message_type(MessageType::STRING);
                            message_info->mutable_message()->mutable_string_message()->set_content(msg.content());
                            break;
                        case MessageType::IMAGE:
                            message_info->mutable_message()->set_message_type(MessageType::IMAGE);
                            message_info->mutable_message()->mutable_image_message()->set_file_id(msg.file_id());
                            message_info->mutable_message()->mutable_image_message()->set_image_content(file_data_lists[msg.file_id()]);
                            break;
                        case MessageType::FILE:
                            message_info->mutable_message()->set_message_type(MessageType::FILE);
                            message_info->mutable_message()->mutable_file_message()->set_file_id(msg.file_id());
                            message_info->mutable_message()->mutable_file_message()->set_file_size(msg.file_size());
                            message_info->mutable_message()->mutable_file_message()->set_file_name(msg.file_name());
                            message_info->mutable_message()->mutable_file_message()->set_file_contents(file_data_lists[msg.file_id()]);
                            break;
                        case MessageType::SPEECH:
                            message_info->mutable_message()->set_message_type(MessageType::SPEECH);
                            message_info->mutable_message()->mutable_speech_message()->set_file_id(msg.file_id());
                            message_info->mutable_message()->mutable_speech_message()->set_file_contents(file_data_lists[msg.file_id()]);
                            break;
                        default:
                            LOG_ERROR("未知的消息类型");
                            return;
                    }

                }
            }
        virtual void MsgSearch(google::protobuf::RpcController* controller,
                            const ::yu::MsgSearchReq* request,
                            ::yu::MsgSearchRsp* response,
                            ::google::protobuf::Closure* done)
            {
                brpc::ClosureGuard rpc_guard(done);
                auto err_ret = [this,response](const std::string &rid,
                                                const std::string &errmsg)
                {
                    response->set_request_id(rid);
                    response->set_success(false);
                    response->set_errmsg(errmsg);
                    return;
                };
                //只针对文本消息
                //1、从请求中取出请求id会话id关键字
                std::string rid = request->request_id();
                std::string session_id = request->chat_session_id();
                std::string keyword = request->search_key();
                //2、从ES中进行查询
                auto msg_lists = _es_message->search(keyword,session_id);
                if(msg_lists.empty())
                {
                    response->set_request_id(rid);
                    response->set_success(true);
                    return;
                }
                //3、组织所有消息用户id从用户子服务中获取信息
                std::unordered_set<std::string> user_ids_lists;
                for(auto &msg:msg_lists)
                {
                    if(msg.user_id().empty())
                    {
                        continue;
                    }
                    user_ids_lists.insert(msg.user_id());
                }
                std::unordered_map<std::string,UserInfo> user_lists;
                bool ret = _getuser(rid,user_ids_lists,user_lists);
                if(!ret)
                {
                    LOG_ERROR("获取用户数据失败",rid);
                    return err_ret(rid,"获取用户数据失败");
                }
                //4、组织响应数据
                response->set_request_id(rid);
                response->set_success(true);
                for(auto &msg:msg_lists)
                {
                    auto message_info = response->add_msg_list();
                    message_info->set_message_id(msg.message_id());
                    message_info->set_chat_session_id(msg.session_id());
                    message_info->set_timestamp(boost::posix_time::to_time_t(msg.create_time()));
                    message_info->mutable_sender()->CopyFrom(user_lists[msg.user_id()]);  
                    message_info->mutable_message()->set_message_type(MessageType::STRING);
                    message_info->mutable_message()->mutable_string_message()->set_content(msg.content());  
                }        
            }
        
        void on_message(const char* body,size_t sz)
        {
            //1、取出文件中的消息，并进行反序列化
            yu::MessageInfo msg;    
            bool ret = msg.ParseFromArray(body,sz);
            if(!ret)
            {
                LOG_ERROR("反序列化消息失败");
                return;
            }
            //2、根据不同消息的类型进行处理
            std::string file_name,file_id,content;
            int64_t file_size;
            switch(msg.message().message_type())
            {
                //1、文本消息存储到ES中
                case MessageType::STRING:
                {
                    content = msg.message().string_message().content();
                    ret = _es_message->appenddata(msg.sender().user_id(),
                                       msg.message_id(),
                                        msg.timestamp(),
                                        content,
                                        msg.chat_session_id());
                    if(!ret)
                    {
                        LOG_ERROR("存储消息到ES失败");
                        return;
                    }
                    break;
                }
                //2、如果是图片/语音/文件消息则使用文件存储子服务来进行存储
                case MessageType::IMAGE:
                {
                    const auto & message = msg.message().image_message();
                    ret = _putfile("",message.image_content().size(),message.image_content(),file_id);
                    if(!ret)
                    {
                        LOG_ERROR("存储图片到文件存储子服务失败");
                        return;
                    }
                }
                break;
                case MessageType::FILE:
                {
                    const auto & message = msg.message().file_message();
                    file_name = message.file_name();
                    file_size = message.file_size();
                    ret = _putfile(file_name,file_size,message.file_contents(),file_id);
                    if(!ret)
                    {
                        LOG_ERROR("存储文件到文件存储子服务失败");
                        return;
                    }
                }
                break;
                case MessageType::SPEECH:
                {
                    const auto & message = msg.message().speech_message();
                    ret = _putfile("",message.file_contents().size(),message.file_contents(),file_id);
                    if(!ret)
                    {
                        LOG_ERROR("存储语音到文件存储子服务失败");
                        return;
                    }
                } 
                break;
            }
            //3、提取元信息放到mysql
            yu::Message message(msg.message_id(),msg.chat_session_id()
            ,msg.sender().user_id(),msg.message().message_type(),boost::posix_time::from_time_t(msg.timestamp()));
            message.file_id(file_id);
            message.file_name(file_name);
            message.file_size(file_size);
            ret = _mysql_message->insert(message);
            if(!ret)
            {
                LOG_ERROR("新增消息出错");
                return;
            }
        }
    private:
        bool _getuser(const std::string rid,
                    std::unordered_set<std::string>& user_ids_lists,
                    std::unordered_map<std::string,UserInfo>& user_lists)
        {
            auto channel = _mm_channels->choose(_user_service_name);
            if(!channel)
            {
                LOG_ERROR("获取用户子服务失败{}",_user_service_name);
                return false;
            }
            yu::UserService_Stub stub(channel.get());
            yu::GetMultiUserInfoReq req;
            yu::GetMultiUserInfoRsp rsp;
            req.set_request_id(rid);
            brpc::Controller cntl;
            stub.GetMultiUserInfo(&cntl,&req,&rsp,nullptr);
            if(cntl.Failed() == true||rsp.success() == false)
            {
                LOG_ERROR("用户子服务调用失败{}-{}",rid,cntl.ErrorText());
                return false;
            }
            const auto& umap = rsp.users_info();
            for(auto it = umap.begin();it != umap.end();it++)
            {
                user_lists.insert(std::make_pair(it->first,it->second));
            }
            return true;
        }
        bool _getfile(const std::string& rid,
           std::unordered_set<std::string>& file_ids_lists,
           std::unordered_map<std::string,std::string>& file_data_lists)
        {
            //批量文件的下载
            auto channel = _mm_channels->choose(_file_service_name);
            if(!channel)
            {
                LOG_ERROR("获取文件存储子服务失败{}",_file_service_name);
                return false;
            }
            yu::FileService_Stub stub(channel.get());
            yu::GetMultiFileReq req;
            yu::GetMultiFileRsp rsp;
            req.set_request_id(rid);
            for(auto& file_id:file_ids_lists)
            {
                req.add_file_id_list(file_id);
            }
            brpc::Controller cntl;
            stub.GetMultiFile(&cntl,&req,&rsp,nullptr);
            if(cntl.Failed() == true||rsp.success() == false)
            {
                LOG_ERROR("文件子服务调用失败{}-{}",rid,cntl.ErrorText());
                return false;
            }
            const auto& fmap = rsp.file_data();
            for(auto it = fmap.begin();it != fmap.end();it++)
            {
                file_data_lists.insert(std::make_pair(it->first,it->second.file_content()));
            }
            return true;
        }
        bool _putfile(const std::string& file_name,
            const int64_t& file_size,const std::string& content,
        std::string &file_id)
        {
            //实现文件的上传
            auto channel = _mm_channels->choose(_file_service_name);
            if(!channel)
            {
                LOG_ERROR("获取文件存储子服务失败{}",_file_service_name);
                return false;
            }
            yu::FileService_Stub stub(channel.get());
            yu::PutSingleFileReq req;
            yu::PutSingleFileRsp rsp;
            req.mutable_file_data()->set_file_name(file_name);
            req.mutable_file_data()->set_file_size(file_size);
            req.mutable_file_data()->set_file_content(content);
            brpc::Controller cntl;
            req.set_request_id(cntl.request_id());
            stub.PutSingleFile(&cntl,&req,&rsp,nullptr);
            if(cntl.Failed() == true||rsp.success() == false)
            {
                LOG_ERROR("文件子服务调用失败{}-{}",cntl.request_id(),cntl.ErrorText());
                return false;
            }
            file_id = rsp.file_info().file_id();
            return true;
        }
    private:
        //存储数据的表
        ESMessage::ptr _es_message;
        Messagetable::ptr _mysql_message;
        //这边是rpc调用客户端相关对象
        std::string _file_service_name;
        ServiceManager::ptr _mm_channels;
        std::string _user_service_name;
    };

    class  MessageServer
    {
    public:
        using ptr = std::shared_ptr<MessageServer>;
        MessageServer( 
            const Discovery::ptr &service_discovery,
            const Registry::ptr &reg_client,
            const std::shared_ptr<elasticlient::Client>& es_client,
            const std::shared_ptr<odb::core::database>& mysql_client,
            const std::shared_ptr<brpc::Server> &server,
            const MQClient::ptr &mq_client)
        :_service_discover(service_discovery)
        ,_reg_client(reg_client)
        ,_es_client(es_client)
        ,_mysql_client(mysql_client)
        ,_rpc_server(server)
        ,_mq_client(mq_client)   
        {}
        ~MessageServer(){}
        //启动服务
        void start()
        {
            _rpc_server->RunUntilAskedToQuit();
        }
    private:
        Discovery::ptr _service_discover;
        Registry::ptr _reg_client;
        //用于存储数据的客户端
        MQClient::ptr _mq_client;
        std::shared_ptr<elasticlient::Client> _es_client;
        std::shared_ptr<odb::core::database> _mysql_client;
        std::shared_ptr<brpc::Server> _rpc_server;
    };
    class  MessageServerBuilder
    {
    public:
         //构造es客户端
         void make_es_client(const  std::vector<std::string> host_list)
        {
            _es_client = ESFactory::create(host_list);
        }
         //构造mysql客户端
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
        //构造消息队列客户端
        void make_mq_client(const std::string &user, 
            const std::string& passwd,
            const std::string& host,
            const std::string& exchange,
            const std::string& queue_name,
            const std::string& binding_key)
        {
            _binding_key = binding_key;
            _queue_name = queue_name;
            _exchange_name = exchange;
            _mq_client = std::make_shared<MQClient>(user,passwd,host);
            _mq_client->declareComponents(exchange,queue_name,binding_key);
        }
         //构造服务发现客户端以及信道管理对象
        void make_discovery_client(const std::string &host,
                                    const std::string &base_service_name,
                                    const std::string &file_service_name
                                ,const std::string &user_service_name)
        {
            _mm_channels = std::make_shared<ServiceManager>();
            _mm_channels->declared(file_service_name);
            _mm_channels->declared(user_service_name);
            auto put_cb = std::bind(&ServiceManager::onServiceOnline, _mm_channels.get(), std::placeholders::_1, std::placeholders::_2);
            auto del_cb = std::bind(&ServiceManager::onServiceOffline, _mm_channels.get(), std::placeholders::_1, std::placeholders::_2);
            _service_discover = std::make_shared<Discovery>(host,base_service_name,put_cb,del_cb);
        }
         //用于构造服务注册客户端对象
        void make_reg(const std::string& etcd_host ,const std::string& service_name,const std::string& access_host)
        {
            if(!_service_discover)
            {
                LOG_ERROR("服务发现客户端未初始化");
                abort();
            }
            _reg_client = std::make_shared<Registry>(etcd_host);
            _reg_client->registry(service_name,access_host);
        }
        //构造RPC服务器对象
        void make_server(uint16_t port, int32_t timeout, uint8_t num_threads)
        {
            if(!_es_client)
            {
                LOG_ERROR("ES搜索引擎未初始化");
                abort();
            }
            if(!_mysql_client)
            {
                LOG_ERROR("mysql客户端未初始化");
                abort();
            }
            if(!_mq_client)
            {
                LOG_ERROR("消息队列客户端未初始化");
                abort();
            }
            _rpc_server = std::make_shared<brpc::Server>();
            MessageServiceIpml *message_service = new MessageServiceIpml(_es_client,
                _mysql_client,_mm_channels,_file_service_name,
                _user_service_name);
            int ret = _rpc_server->AddService(message_service, brpc::ServiceOwnership::SERVER_DOESNT_OWN_SERVICE);
            if (ret == -1) 
            {
                LOG_ERROR("添加RPC服务器失败");
                abort();
            }
            brpc::ServerOptions options;
            options.idle_timeout_sec = timeout; //连接空闲超时时间-超时后连接被关闭
            options.num_threads = num_threads; // io线程数量
            ret = _rpc_server->Start(port, &options);
            if (ret == -1) {
                LOG_ERROR("服务启动失败！");
                abort();
            }
            auto cb = std::bind(&MessageServiceIpml::on_message,message_service
                ,std::placeholders::_1,std::placeholders::_2);
            _mq_client->consume(_queue_name,cb);

        }
        MessageServer::ptr build()
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
            MessageServer::ptr server = std::make_shared<MessageServer>(_service_discover,
                _reg_client,_es_client,_mysql_client
                ,_rpc_server,_mq_client);
            return server;
        }
    private:
        Registry::ptr _reg_client;
        std::shared_ptr<elasticlient::Client> _es_client;
        std::shared_ptr<odb::core::database> _mysql_client;
        std::string _file_service_name;
        ServiceManager::ptr _mm_channels;
        Discovery::ptr _service_discover;
        std::string _user_service_name;
        std::string _binding_key;
        std::string _queue_name;
        std::string _exchange_name;
        MQClient::ptr _mq_client;

        std::shared_ptr<brpc::Server> _rpc_server;
        
    };
}
