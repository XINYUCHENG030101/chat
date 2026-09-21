#pragma once

#include <jsoncpp/json/json.h>
#include"logger.hpp" //日志
#include"asr.hpp"   //语音识别
#include"speech.pb.h"
#include"etcd.hpp" //服务注册
#include <brpc/server.h>
#include <butil/logging.h>
#include <iostream>

namespace yu
{
    class SpeechServiceIpml:public yu::SpeechService
    {
    public:
        SpeechServiceIpml(const AsrClient::ptr& asr_client)
            :_asr_client(asr_client)
            {}
        ~SpeechServiceIpml(){}
        void SpeechRecognition(google::protobuf::RpcController* controller,
                       const ::yu::SpeechRecognitionReq* req,
                       ::yu::SpeechRecognitionRsp* rsp,
                       ::google::protobuf::Closure* done) 
        {
            brpc::ClosureGuard rpc_guard(done);
            //取出语音数据
            //识别
            std::string err;
            std::string res = _asr_client->recognize(req->speech_content(),err);
            if(res.empty())
            {
                LOG_ERROR("{}语言识别处理失败",req->request_id());
                rsp->set_request_id(req->request_id());
                rsp->set_success(false);
                rsp->set_errmsg("语音识别失败"+ err);
            }
            //构造相关响应
            rsp->set_request_id(req->request_id());
            rsp->set_success(true);
            rsp->set_recognition_result(res);
            
        }
    private:
        AsrClient::ptr _asr_client;
    };

    class  SpeechServer
    {
    public:
        using ptr = std::shared_ptr<SpeechServer>;
        SpeechServer(const AsrClient::ptr asr_client, 
            const Registry::ptr &reg_client,
            const std::shared_ptr<brpc::Server> &server)
        :_asr_client(asr_client)
        ,_reg_client(reg_client)
        ,_rpc_server(server)
        {}
        ~SpeechServer(){}
        //启动服务
        void start()
        {
            _rpc_server->RunUntilAskedToQuit();
        }
    private:
        AsrClient::ptr _asr_client;
        Registry::ptr _reg_client;
        std::shared_ptr<brpc::Server> _rpc_server;
    };
    class  SpeechServerBuilder
    {
    public:
        //构造语音识别客户端对象
        void make_asr(const std::string &app_id, const std::string& api_key,const std::string& secret_key)
        {
            _asr_client = std::make_shared<AsrClient>(app_id, api_key, secret_key);
        }
         //用于构造服务注册客户端对象
        void make_reg(const std::string& etcd_host ,const std::string& service_name,const std::string& access_host)
        {
            _reg_client = std::make_shared<Registry>(etcd_host);
            _reg_client->registry(service_name,access_host);
        }
        //构造RPC服务器对象
        void make_server(uint16_t port, int32_t timeout, uint8_t num_threads)
        {
            if (!_asr_client) {
                LOG_ERROR("还未初始化语音识别模块！");
                abort();
            }
            _rpc_server = std::make_shared<brpc::Server>();
            SpeechServiceIpml *speech_service = new SpeechServiceIpml(_asr_client);
            int ret = _rpc_server->AddService(speech_service, brpc::ServiceOwnership::SERVER_DOESNT_OWN_SERVICE);
            if (ret == -1) 
            {if (!_asr_client) {
                            LOG_ERROR("还未初始化语音识别模块！");
                            abort();
                        }
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
        SpeechServer::ptr build()
        {
            if(!_asr_client)
            {
                LOG_ERROR("语音识别服务创建失败");
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
            SpeechServer::ptr server = std::make_shared<SpeechServer>(_asr_client, _reg_client, _rpc_server);
            return server;
        }

    private:
        AsrClient::ptr _asr_client;
        Registry::ptr _reg_client;
        std::shared_ptr<brpc::Server> _rpc_server;
    };
}