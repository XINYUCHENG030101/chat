#pragma once

#include <jsoncpp/json/json.h>
#include"logger.hpp" //日志
#include"file.pb.h"
#include"base.pb.h"
#include"etcd.hpp" //服务注册
#include <brpc/server.h>
#include <butil/logging.h>
#include <iostream>
#include"utils.hpp"
namespace yu
{
    class FileServiceIpml:public yu::FileService
    {
    public:
        FileServiceIpml(const std::string &storage_path)
        :_storage_path(storage_path)    
        {
            umask(0);
            mkdir(storage_path.c_str(),0755);
            if (_storage_path.back() != '/') _storage_path.push_back('/');
        }
        ~FileServiceIpml(){}
        void GetSingleFile(google::protobuf::RpcController* controller,
                       const ::yu::GetSingleFileReq* req,
                       ::yu::GetSingleFileRsp* rsp,
                       ::google::protobuf::Closure* done) 
        {
            brpc::ClosureGuard rpc_guard(done);
            //1. 取出请求中的文件ID（起始就是文件名）
            rsp->set_request_id(req->request_id());
            std::string fid = req->file_id();
            std::string filename = fid;
            std::string body;
            bool ret = readfile(filename,body);
            if(ret == false)
            {
                LOG_ERROR("读取文件失败{}",req->request_id());
                rsp->set_success(false);
                rsp->set_errmsg("读取文件失败");
                return ;
            }

            rsp->set_success(true);
            rsp->mutable_file_data()->set_file_id(fid);
            rsp->mutable_file_data()->set_file_content(body);
     
        }
        void GetMultiFile(google::protobuf::RpcController* controller,
                       const ::yu::GetMultiFileReq* req,
                       ::yu::GetMultiFileRsp* rsp,
                       ::google::protobuf::Closure* done) 
        {
            brpc::ClosureGuard rpc_guard(done);
            rsp->set_request_id(req->request_id());
            for(int i = 0;i < req->file_id_list_size();i++)
            {
                std::string fid = req->file_id_list(i);
                std::string filename = fid;
                std::string body;
                bool ret = readfile(filename,body);
                if(ret == false)
                {
                    LOG_ERROR("读取文件失败{}",req->request_id());
                    rsp->set_success(false);
                    rsp->set_errmsg("读取文件失败");
                    return ;
                }
                FileDownloadData data;
                data.set_file_id(fid);
                data.set_file_content(body);
                rsp->mutable_file_data()->insert({fid,data});
            }
            rsp->set_success(true);

        }
        void PutSingleFile(google::protobuf::RpcController* controller,
                       const ::yu::PutSingleFileReq* req,
                       ::yu::PutSingleFileRsp* rsp,
                       ::google::protobuf::Closure* done) 
        {
            brpc::ClosureGuard rpc_guard(done);
            rsp->set_request_id(req->request_id());
            //为文件设置一个uuid
            std::string fid = uuid();
            std::string filename = fid;
            //取出数据写入
            bool ret = writefile(filename,req->file_data().file_content());
            if(ret == false)
            {
                rsp->set_success(false);
                rsp->set_errmsg("写入数据失败");
                LOG_ERROR("写入数据失败{}",req->request_id());
                return;
            }
            //构造响应
            rsp->set_success(true);
            rsp->mutable_file_info()->set_file_id(fid);
            rsp->mutable_file_info()->set_file_size(req->file_data().file_size());
            rsp->mutable_file_info()->set_file_name(req->file_data().file_name());

            
        }
        void PutMultiFile(google::protobuf::RpcController* controller,
                       const ::yu::PutMultiFileReq* req,
                       ::yu::PutMultiFileRsp* rsp,
                       ::google::protobuf::Closure* done) 
        {
            brpc::ClosureGuard rpc_guard(done);
            rsp->set_request_id(req->request_id());
            for(int i = 0;i<req->file_data_size();i++)
            {
                std::string fid = uuid();
                std::string filename = fid;
                bool ret = writefile(filename,req->file_data(i).file_content());
                if(ret == false)
                {
                    rsp->set_success(false);
                    rsp->set_errmsg("写入数据失败");
                    LOG_ERROR("写入数据失败{}",req->request_id());
                    return;
                }
                yu::FileMessageInfo *info  = rsp->add_file_info();
                info->set_file_id(fid);
                info->set_file_size(req->file_data(i).file_size());
                info->set_file_name(req->file_data(i).file_name());
            }
            rsp->set_success(true);
        
        }
    private:
        std::string _storage_path;
    };

    class  FileServer
    {
    public:
        using ptr = std::shared_ptr<FileServer>;
        FileServer(const Registry::ptr &reg_client,const std::shared_ptr<brpc::Server> &server)
        :_reg_client(reg_client)
        ,_rpc_server(server)
        {}
        ~FileServer(){}
        //启动服务
        void start()
        {
            _rpc_server->RunUntilAskedToQuit();
        }
    private:
        Registry::ptr _reg_client;
        std::shared_ptr<brpc::Server> _rpc_server;
    };
    class  FileServerBuilder
    {
    public:
       
         //用于构造服务注册客户端对象
        void make_reg(const std::string& etcd_host ,const std::string& service_name,const std::string& access_host)
        {
            _reg_client = std::make_shared<Registry>(etcd_host);
            _reg_client->registry(service_name,access_host);
        }
        //构造RPC服务器对象
        void make_server(uint16_t port, int32_t timeout, uint8_t num_threads, const std::string &path = "./data/")
        {
            _rpc_server = std::make_shared<brpc::Server>();
            FileServiceIpml *file_service = new FileServiceIpml(path);
            int ret = _rpc_server->AddService(file_service, brpc::ServiceOwnership::SERVER_DOESNT_OWN_SERVICE);
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
        FileServer::ptr build()
        {
            
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
            FileServer::ptr server = std::make_shared<FileServer>(_reg_client, _rpc_server);
            return server;
        }

    private:
        Registry::ptr _reg_client;
        std::shared_ptr<brpc::Server> _rpc_server;
    };
}
