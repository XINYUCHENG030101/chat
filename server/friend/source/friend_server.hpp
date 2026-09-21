#pragma once
#include<memory>
#include"user.hxx"
#include"user-odb.hxx"
#include <jsoncpp/json/json.h>
#include"logger.hpp" //日志
#include"usr.pb.h"
#include"message.pb.h"
#include"base.pb.h"
#include"friend.pb.h"
#include"etcd.hpp" //服务注册
#include <brpc/server.h>
#include <butil/logging.h>
#include <iostream>
#include"mysql.hpp"
#include"data_es.hpp"
#include"mysql_user.hpp"
#include"channel.hpp"
#include"utils.hpp"
#include"mysql_apply.hpp"
#include"mysql_relation.hpp"
#include"mysql_session.hpp"
#include"mysql_session_member.hpp"
namespace yu
{
    class FriendServiceIpml:public yu::FriendService
    {
    public:
        FriendServiceIpml(
            const std::shared_ptr<elasticlient::Client> &es_client,
            const std::shared_ptr<odb::core::database> &db,
            const ServiceManager::ptr &channel_manager,
            const std::string &user_service_name,
            const std::string &message_service_name
        )
        :_es_user(std::make_shared<ESuser>(es_client)),  
        _mysql_apply(std::make_shared<Applytable>(db)),
        _mysql_relation(std::make_shared<Relationtable>(db)),
        _mysql_chat_session(std::make_shared<Chatsessiontable>(db)),
        _mysql_chat_session_member(std::make_shared<ChatSessionMemberTable>(db)),
        _user_service_name(user_service_name),
        _message_service_name(message_service_name),
        _mm_channels(channel_manager)
        {}
        ~FriendServiceIpml(){}
        
        virtual void GetFriendList(::google::protobuf::RpcController* controller,
                       const ::yu::GetFriendListReq* request,
                       ::yu::GetFriendListRsp* response,
                       ::google::protobuf::Closure* done)
            {
                brpc::ClosureGuard rpc_guard(done);
                //1. 定义错误回调
                auto err_response = [this, response](const std::string &rid, 
                    const std::string &errmsg) -> void 
                {
                    response->set_request_id(rid);
                    response->set_success(false);
                    response->set_errmsg(errmsg);
                    return;
                };
                //1. 获取请求中的用户ID
                std::string rid = request ->request_id();
                std::string uid = request->user_id();
                //2. 根据用户ID，从数据库的好友关系表取出该用户所有的好友简息
                auto firend_lists = _mysql_relation->friends(uid);
                if(firend_lists.empty())
                {
                    err_response(rid,"获取好友列表失败");
                    return;
                }
                std::unordered_set<std::string> uid_list;
                std::unordered_map<std::string,UserInfo> user_list;
                for(const auto& it:firend_lists)
                {
                    uid_list.insert(it);
                }
                //3. 根据用户子服务中获取用户信息
                bool ret = GetUserInfo(rid,uid_list,user_list);
                if(ret == false)
                {
                    LOG_ERROR("获取用户信息失败{}",rid);
                    err_response(rid,"获取用户信息失败");
                    return;
                }
                //4. 组织响应，将好友列表返回给网关。
                response->set_request_id(rid);
                response->set_success(true);
                for(const auto&it:user_list)
                {
                    auto user = response->add_friend_list();
                    user->CopyFrom(it.second);
                }
            }
        virtual void FriendRemove(::google::protobuf::RpcController* controller,
                       const ::yu::FriendRemoveReq* request,
                       ::yu::FriendRemoveRsp* response,
                       ::google::protobuf::Closure* done)
            {
                brpc::ClosureGuard rpc_guard(done);
                //1. 定义错误回调
                auto err_response = [this, response](const std::string &rid, 
                    const std::string &errmsg) -> void 
                {
                    response->set_request_id(rid);
                    response->set_success(false);
                    response->set_errmsg(errmsg);
                    return;
                };
                //1. 取出请求中的删除者ID和被删除者ID
                std::string rid = request->request_id();
                std::string uid = request->user_id();
                std::string pid = request->peer_id();
                //2. 从用户好友关系表中删除相关关系数据，从会话表中删除单聊会话
                bool ret = _mysql_relation->remove(uid,pid);
                if(ret == false)
                {
                    err_response(rid,"操作关系表删除好友关系失败");
                    LOG_ERROR("操作关系表删除好友关系失败{}",rid);
                    return;
                }
                ret = _mysql_chat_session->remove(uid,pid);
                if(ret == false)
                {
                    err_response(rid,"操作会话表删除会话失败");
                    LOG_ERROR("操作会话表删除会话失败{}",rid);
                    return;
                }
                //3. 组织响应，返回给网关
                response->set_request_id(rid);
                response->set_success(true);
            }
        virtual void FriendAdd(::google::protobuf::RpcController* controller,
                       const ::yu::FriendAddReq* request,
                       ::yu::FriendAddRsp* response,
                       ::google::protobuf::Closure* done)
            {
                brpc::ClosureGuard rpc_guard(done);
                //1. 定义错误回调
                auto err_response = [this, response](const std::string &rid, 
                    const std::string &errmsg) -> void 
                {
                    response->set_request_id(rid);
                    response->set_success(false);
                    response->set_errmsg(errmsg);
                    return;
                };
                //1. 取出请求中的请求者ID，和被请求者ID
                std::string rid = request->request_id();
                std::string uid = request->user_id();//申请人
                std::string pid = request->respondent_id();//被申请人
                //2. 判断两人是否已经是好友
                bool ret = _mysql_relation->exist(uid,pid);
                if(ret == true)
                {
                    err_response(rid,"申请人和被申请人已经是好友");
                    LOG_ERROR("申请人和被申请人已经是好友{}-{}",uid,pid);
                    return;
                }
                //3. 判断该用户是否已经申请过好友关系
                ret = _mysql_apply->exist(uid,pid);
                if(ret == true)
                {
                    err_response(rid,"申请人已经申请过好友关系");
                    LOG_ERROR("申请人已经申请过好友关系{}-{}",uid,pid);
                    return;
                }
                //4. 向好友申请事件表中，新增申请信息
                std::string eid = uuid();
                yu::Friend_apply apply(eid,uid,pid);
                ret = _mysql_apply->insert(apply);
                if(ret == false)
                {
                    err_response(rid,"操作好友申请表新增申请失败");
                    LOG_ERROR("操作好友申请表新增申请失败{}",rid);
                    return;
                }
                //5. 组织响应，将事件ID信息响应给网关
                response->set_request_id(rid);
                response->set_success(true);
                response->set_notify_event_id(eid);
            }
        virtual void FriendAddProcess(::google::protobuf::RpcController* controller,
                       const ::yu::FriendAddProcessReq* request,
                       ::yu::FriendAddProcessRsp* response,
                       ::google::protobuf::Closure* done)
            {
                brpc::ClosureGuard rpc_guard(done);
                //1. 定义错误回调
                auto err_response = [this, response](const std::string &rid, 
                    const std::string &errmsg) -> void 
                {
                    response->set_request_id(rid);
                    response->set_success(false);
                    response->set_errmsg(errmsg);
                    return;
                };
                //1. 取出请求中的用户ID
                std::string rid = request->request_id();
                std::string uid = request->apply_user_id();//申请人
                std::string pid = request->user_id();//被申请人
                std::string eid = request->notify_event_id();//申请事件ID
                bool agree = request->agree();//是否同意
                //2.判断有没有该申请事件 
                bool ret = _mysql_apply->exist(eid);
                if(ret == false)
                {
                    err_response(rid,"申请事件不存在");
                    LOG_ERROR("申请事件不存在{}",rid);
                    return;
                }
                //3.如果有：可以处理 ---删除处理事件--事件已经处理完毕
                ret = _mysql_apply->remove(uid,pid);
                if(ret == false)
                {
                    err_response(rid,"操作好友申请表删除申请失败");
                    LOG_ERROR("操作好友申请表删除申请失败{}",rid);
                    return;
                }
                //如果处理结果是同意，向数据库中新增好友信息，新增单聊会话和会话成员
                std::string cssid;
                if(agree == true)
                {
                    ret = _mysql_relation->insert(uid,pid);
                    if(ret == false)
                    {
                        err_response(rid,"操作好友关系表新增好友失败");
                        LOG_ERROR("操作好友关系表新增好友失败{}",rid);
                        return;
                    }
                    cssid = uuid();
                    Chat_session cs(cssid,"",yu::Chatsessiontype::Single);
                    ret = _mysql_chat_session->insert(cs);
                    if(ret == false)
                    {
                        err_response(rid,"操作会话表新增会话失败");
                        LOG_ERROR("操作会话表新增会话失败{}",rid);
                        return;
                    }
                    ChatSessionMember csm1(uid, cssid);
                    ChatSessionMember csm2(pid, cssid);
                    std::vector<ChatSessionMember> csm_list = {csm1,csm2};
                    ret = _mysql_chat_session_member->append(csm_list);
                    if(ret == false)
                    {
                        err_response(rid,"操作会话成员表新增会话成员失败");
                        LOG_ERROR("操作会话成员表新增会话成员失败{}",rid);
                        return;
                    }
                }
                //如果拒绝无需任何操作
                //4. 组织响应，将申请事件列表响应给网关
                response->set_request_id(rid);
                response->set_success(true);
                response->set_new_session_id(cssid);
            }
        virtual void FriendSearch(::google::protobuf::RpcController* controller,
                       const ::yu::FriendSearchReq* request,
                       ::yu::FriendSearchRsp* response,
                       ::google::protobuf::Closure* done)
            {
                brpc::ClosureGuard rpc_guard(done);
                //1. 定义错误回调
                auto err_response = [this, response](const std::string &rid, 
                    const std::string &errmsg) -> void 
                {
                    response->set_request_id(rid);
                    response->set_success(false);
                    response->set_errmsg(errmsg);
                    return;
                };
                //拿到搜索关键字，请求id，用户id
                std::string rid = request->request_id();
                std::string uid = request->user_id();
                std::string keyword = request->search_key();
                //获取用户好友列表
                auto res = _mysql_relation->friends(uid);
                if(res.empty() == true)
                {
                    err_response(rid,"用户好友列表为空");
                    LOG_ERROR("用户好友列表为空{}",rid);
                    return;
                }
                std::unordered_set<std::string> user_id_list;
                res.push_back(uid);
                //搜索
                auto search_res = _es_user->search(keyword,res);
                if(search_res.empty() == true)
                {
                    err_response(rid,"搜索用户失败");
                    LOG_ERROR("搜索用户失败{}",rid);
                    return;
                }
                for(auto &it:search_res)
                {
                    user_id_list.insert(it.user_id());
                }
                //根据获取到的列表使用用户子服务进行批量搜索
                std::unordered_map<std::string,UserInfo> user_list;
                bool ret = GetUserInfo(rid,user_id_list,user_list);
                if(ret == false)
                {
                    err_response(rid,"获取用户信息失败");
                    LOG_ERROR("获取用户信息失败{}",rid);
                    return;
                }
                //构造响应
                response->set_request_id(rid);
                response->set_success(true);
                for(auto &it:user_list)
                {
                    auto user_info = response->add_user_info();
                    user_info->CopyFrom(it.second);
                }
            }
        virtual void GetChatSessionList(::google::protobuf::RpcController* controller,
                       const ::yu::GetChatSessionListReq* request,
                       ::yu::GetChatSessionListRsp* response,
                       ::google::protobuf::Closure* done)
            {
                brpc::ClosureGuard rpc_guard(done);
                //1. 定义错误回调
                auto err_response = [this, response](const std::string &rid, 
                    const std::string &errmsg) -> void 
                {
                    response->set_request_id(rid);
                    response->set_success(false);
                    response->set_errmsg(errmsg);
                    return;
                };
                //作用：一个用户登录成功后，能够展示自己的历史聊天信息
                //提取请求内容：用户id
                std::string rid = request->request_id();
                std::string uid = request->user_id();
                //查询单聊列表
                    //从单聊会话列表中取出所有好友id,从用户子服务中获取用户信息
                auto sf_list = _mysql_chat_session->SingleChatSession(uid);
                std::unordered_set<std::string> uid_list;
                for(auto &it:sf_list)
                {
                    uid_list.insert(it.friend_id);
                }
                std::unordered_map<std::string,UserInfo> user_list;
                bool ret = GetUserInfo(rid,uid_list,user_list);
                if(ret == false)
                {
                    err_response(rid,"获取用户信息失败");
                    LOG_ERROR("获取用户信息失败{}",rid);
                    return;
                }
                //设置响应会话信息：会话名称为好友名称。会话头像是好友头像
                //查询多聊列表
                auto gs_list = _mysql_chat_session->GroupChatSession(uid);
                //根据所有的会话id从消息存储中获取会话的最后一条消息
                for(auto &it:sf_list)
                {
                    auto chat_session_info = response->add_chat_session_info_list();
                    chat_session_info->set_single_chat_friend_id(it.friend_id);
                    chat_session_info->set_chat_session_id(it.chat_session_id);
                    chat_session_info->set_chat_session_name(user_list[it.friend_id].nickname());
                    chat_session_info->set_avatar(user_list[it.friend_id].avatar());
                    MessageInfo msg;
                    ret = GetRecentMsg(rid,it.chat_session_id,msg);
                    if(ret ==false)
                    {
                        continue;
                    }
                    chat_session_info->mutable_prev_message()->CopyFrom(msg);
                }
                for(auto &it:gs_list)
                {
                    auto chat_session_info = response->add_chat_session_info_list();
                    chat_session_info->set_chat_session_id(it.chat_session_id);
                    chat_session_info->set_chat_session_name(it.chat_session_name);
                    MessageInfo msg;
                    ret = GetRecentMsg(rid,it.chat_session_id,msg);
                    if(ret ==false)
                    {
                        continue;
                    }
                    chat_session_info->mutable_prev_message()->CopyFrom(msg);
                }
                //组织响应
                response->set_request_id(rid);
                response->set_success(true);   
            }
        virtual void ChatSessionCreate(::google::protobuf::RpcController* controller,
                       const ::yu::ChatSessionCreateReq* request,
                       ::yu::ChatSessionCreateRsp* response,
                       ::google::protobuf::Closure* done)
            {
                brpc::ClosureGuard rpc_guard(done);
                //1. 定义错误回调
                auto err_response = [this, response](const std::string &rid, 
                    const std::string &errmsg) -> void 
                {
                    response->set_request_id(rid);
                    response->set_success(false);
                    response->set_errmsg(errmsg);
                    return;
                };
                //创建会话，针对创建群聊会话
                //提取请求参数，会话名称，会话成员，请求id
                std::string rid = request->request_id();
                std::string uid = request->user_id();
                std::string cssname = request->chat_session_name();
                //生成数据id，向数据库中新增会话信息，会话成员信息
                std::string cssid = uuid();
                Chat_session cs(cssid,cssname,Chatsessiontype::Group);
                bool ret = _mysql_chat_session->insert(cs);
                if(ret == false)
                {
                    err_response(rid,"新增会话失败");
                    LOG_ERROR("新增会话失败{}-{}",rid,cssname);
                    return;
                }
                std::vector<ChatSessionMember> member_list;
                for (int i = 0; i < request->member_id_list_size(); i++) 
                {
                    ChatSessionMember csm(request->member_id_list(i), cssid);
                    member_list.push_back(csm);
                }
                ret  = _mysql_chat_session_member->append(member_list);
                if(ret == false)
                {
                    err_response(rid,"新增会话成员失败");
                    LOG_ERROR("新增会话成员失败{}-{}",rid,cssname);
                    return;
                }
                //组织响应，组织会话信息
                response->set_request_id(rid);
                response->set_success(true);
                response->mutable_chat_session_info()->set_chat_session_id(cssid);
                response->mutable_chat_session_info()->set_chat_session_name(cssname);
            }
        virtual void GetChatSessionMember(::google::protobuf::RpcController* controller,
                       const ::yu::GetChatSessionMemberReq* request,
                       ::yu::GetChatSessionMemberRsp* response,
                       ::google::protobuf::Closure* done)
            {
                brpc::ClosureGuard rpc_guard(done);
                //1. 定义错误回调
                auto err_response = [this, response](const std::string &rid, 
                    const std::string &errmsg) -> void 
                {
                    response->set_request_id(rid);
                    response->set_success(false);
                    response->set_errmsg(errmsg);
                    return;
                };
                //用于用户查看群聊成员信息，进行成员信息展示
                //提取请求内容，请求id，聊天会话id
                std::string rid = request->request_id();
                std::string cssid = request->chat_session_id();
                std::string uid = request->user_id();
                //从数据库中获取会话成员id列表
                std::unordered_set<std::string> uid_list;
                auto member_id_list = _mysql_chat_session_member->get_user_id(cssid);
                for(const auto &it:member_id_list)
                {
                    uid_list.insert(it);
                }
                std::unordered_map<std::string,UserInfo> user_list;
                //从用户子服务中批量获取用户信息
                bool ret = GetUserInfo(rid,uid_list,user_list);
                if(ret == false)
                {
                    err_response(rid,"获取用户信息失败");
                    LOG_ERROR("获取用户信息失败{}",rid);
                    return;
                }
                //构造响应
                response->set_request_id(rid);
                response->set_success(true);
                for(auto &it:user_list)
                {
                    auto user_info = response->add_member_info_list();
                    user_info->CopyFrom(it.second);
                }
            }
        virtual void GetPendingFriendEventList(::google::protobuf::RpcController* controller,
                       const ::yu::GetPendingFriendEventListReq* request,
                       ::yu::GetPendingFriendEventListRsp* response,
                       ::google::protobuf::Closure* done)
            {
                brpc::ClosureGuard rpc_guard(done);
                //1. 定义错误回调
                auto err_response = [this, response](const std::string &rid, 
                    const std::string &errmsg) -> void 
                {
                    response->set_request_id(rid);
                    response->set_success(false);
                    response->set_errmsg(errmsg);
                    return;
                };
                //取出用户id
                std::string uid = request->user_id();
                std::string rid = request->request_id();
                //向数据库中获取事件信息
                auto res = _mysql_apply->applyuser(uid);
                std::unordered_map<std::string,UserInfo> user_list;
                std::unordered_set<std::string> uid_list;
                for(auto &it:res)
                {
                    uid_list.insert(it);
                }
                //根据获取到的列表使用用户子服务进行批量搜索
                bool ret = GetUserInfo(rid,uid_list,user_list);
                if(ret == false)
                {
                    err_response(rid,"获取用户信息失败");
                    LOG_ERROR("获取用户信息失败{}",rid);
                    return;
                }
                //构造响应
                response->set_request_id(rid);
                response->set_success(true);
                for(auto &it:user_list)
                {
                    auto user_info = response->add_event();
                    user_info->mutable_sender()->CopyFrom(it.second);
                }
            }
    private:
        bool GetRecentMsg(const std::string& rid
            ,const std::string& cssid,MessageInfo &msg)
        {
             auto channel = _mm_channels->choose(_message_service_name);
                if(!channel)
                {
                    LOG_ERROR("未找到消息子服务节点{}",rid);
                    return false;
                }
                yu::GetRecentMsgReq req;
                yu::GetRecentMsgRsp rsp;
                req.set_request_id(rid);
                req.set_chat_session_id(cssid);
                req.set_msg_count(1);
                yu::MsgStorageService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.GetRecentMsg(&cntl,&req,&rsp,nullptr);
                if(cntl.Failed() == true)
                {
                    LOG_ERROR("获取消息失败{}-{}",rid,cntl.ErrorText());
                    return false;
                }
                if(rsp.success() == false)
                {
                    LOG_ERROR("获取消息失败{}-{}",rid,rsp.errmsg());
                    return false;
                }
                if(rsp.msg_list_size() > 0)
                {
                    msg.CopyFrom(rsp.msg_list(0));
                    return true;
                }
                return false;    
        }
        bool GetUserInfo(const std::string& rid,
                        const std::unordered_set<std::string>& uid_list,
                        std::unordered_map<std::string,UserInfo>& user_list)
        {
               auto channel = _mm_channels->choose(_user_service_name);
                if(!channel)
                {
                    LOG_ERROR("未找到用户子服务节点{}",rid);
                    return false;
                }
                yu::UserService_Stub stub(channel.get());
                yu::GetMultiUserInfoReq req;
                yu::GetMultiUserInfoRsp rsp;
                req.set_request_id(rid);
                for(auto& list:uid_list)
                {
                    req.add_users_id(list);
                }
                brpc::Controller cntl;
                stub.GetMultiUserInfo(&cntl,&req,&rsp,nullptr);
                if(cntl.Failed() == true)
                {
                    LOG_ERROR("获取用户信息失败{}-{}",rid,cntl.ErrorText());
                    return false;
                }
                if(rsp.success() == false)
                {
                    LOG_ERROR("获取用户信息失败{}-{}",rid,rsp.errmsg());
                    return false;
                }
                for(const auto& it:rsp.users_info())
                {
                    user_list.insert(std::make_pair(it.first,it.second));
                }
                return true;
        }
    private:
        //存储数据的表
        ESuser::ptr _es_user;
        Applytable::ptr _mysql_apply;
        Relationtable::ptr _mysql_relation;
        Chatsessiontable::ptr _mysql_chat_session;
        ChatSessionMemberTable::ptr _mysql_chat_session_member;
        //这边是rpc调用客户端相关对象
        std::string _user_service_name;
        std::string _message_service_name;
        ServiceManager::ptr _mm_channels;
        
    };

    class  FriendServer
    {
    public:
        using ptr = std::shared_ptr<FriendServer>;
        FriendServer( 
            const Discovery::ptr &service_discovery,
            const Registry::ptr &reg_client,
            const std::shared_ptr<elasticlient::Client>& es_client,
            const std::shared_ptr<odb::core::database>& mysql_client,
            const std::shared_ptr<brpc::Server> &server)
        :_service_discover(service_discovery)
        ,_reg_client(reg_client)
        ,_es_client(es_client)
        ,_mysql_client(mysql_client)
        ,_rpc_server(server)
        {}
        ~FriendServer(){}
        //启动服务
        void start()
        {
            _rpc_server->RunUntilAskedToQuit();
        }
    private:
        Discovery::ptr _service_discover;
        Registry::ptr _reg_client;
        //用于存储数据的客户端
        std::shared_ptr<elasticlient::Client> _es_client;
        std::shared_ptr<odb::core::database> _mysql_client;
        std::shared_ptr<brpc::Server> _rpc_server;
    };
    class  FriendServerBuilder
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
        
         //构造服务发现客户端以及信道管理对象
        void make_discovery_client(const std::string &host,
                                    const std::string &base_service_name,
                                    const std::string &user_service_name,
                                 const std::string &message_service_name)
        {
            _user_service_name = user_service_name;
            _message_service_name = message_service_name;
            _mm_channels = std::make_shared<ServiceManager>();
            _mm_channels->declared(user_service_name);
            _mm_channels->declared(message_service_name);
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
            _rpc_server = std::make_shared<brpc::Server>();
            FriendServiceIpml *firend_service = new FriendServiceIpml(_es_client,_mysql_client,_mm_channels,_user_service_name,_message_service_name);
            int ret = _rpc_server->AddService(firend_service, brpc::ServiceOwnership::SERVER_DOESNT_OWN_SERVICE);
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
        FriendServer::ptr build()
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
            FriendServer::ptr server = std::make_shared<FriendServer>(_service_discover,
                _reg_client,_es_client,_mysql_client,_rpc_server);
            return server;
        }

    private:
        Registry::ptr _reg_client;
        std::shared_ptr<elasticlient::Client> _es_client;
        std::shared_ptr<odb::core::database> _mysql_client;
        std::string _user_service_name;
        std::string _message_service_name;
        ServiceManager::ptr _mm_channels;
        Discovery::ptr _service_discover;
        std::shared_ptr<brpc::Server> _rpc_server;
    };
}
