#include "data_redis.hpp"      // redis数据管理客户端封装
#include "etcd.hpp"     // 服务注册模块封装
#include "logger.hpp"   // 日志模块封装
#include "channel.hpp"  // 信道管理模块封装

#include "connection.hpp"

#include "usr.pb.h"  // protobuf框架代码
#include "base.pb.h"  // protobuf框架代码
#include "file.pb.h"  // protobuf框架代码
#include "friend.pb.h"  // protobuf框架代码
#include "getway.pb.h"  // protobuf框架代码
#include "message.pb.h"  // protobuf框架代码
#include "speech.pb.h"  // protobuf框架代码
#include "transmit.pb.h"  // protobuf框架代码
#include "notify.pb.h"

#include "httplib.h"

namespace yu
{
    #define GET_PHONE_VERIFY_CODE   "/service/user/get_phone_verify_code"
    #define USERNAME_REGISTER       "/service/user/username_register"
    #define USERNAME_LOGIN          "/service/user/username_login"
    #define PHONE_REGISTER          "/service/user/phone_register"
    #define PHONE_LOGIN             "/service/user/phone_login"
    #define GET_USERINFO            "/service/user/get_user_info"
    #define SET_USER_AVATAR         "/service/user/set_avatar"
    #define SET_USER_NICKNAME       "/service/user/set_nickname"
    #define SET_USER_DESC           "/service/user/set_description"
    #define SET_USER_PHONE          "/service/user/set_phone"
    #define FRIEND_GET_LIST         "/service/friend/get_friend_list"
    #define FRIEND_APPLY            "/service/friend/add_friend_apply"
    #define FRIEND_APPLY_PROCESS    "/service/friend/add_friend_process"
    #define FRIEND_REMOVE           "/service/friend/remove_friend"
    #define FRIEND_SEARCH           "/service/friend/search_friend"
    #define FRIEND_GET_PENDING_EV   "/service/friend/get_pending_friend_events"
    #define CSS_GET_LIST            "/service/friend/get_chat_session_list"
    #define CSS_CREATE              "/service/friend/create_chat_session"
    #define CSS_GET_MEMBER          "/service/friend/get_chat_session_member"
    #define MSG_GET_RANGE           "/service/message_storage/get_history"
    #define MSG_GET_RECENT          "/service/message_storage/get_recent"
    #define MSG_KEY_SEARCH          "/service/message_storage/search_history"
    #define NEW_MESSAGE             "/service/message_transmit/new_message"
    #define FILE_GET_SINGLE         "/service/file/get_single_file"
    #define FILE_GET_MULTI          "/service/file/get_multi_file"
    #define FILE_PUT_SINGLE         "/service/file/put_single_file"
    #define FILE_PUT_MULTI          "/service/file/put_multi_file"
    #define SPEECH_RECOGNITION      "/service/speech/recognition"
    class GatewayServer
    {
    public:
        using ptr = std::shared_ptr<GatewayServer>;
        GatewayServer(
            int websocket_port,
                int http_port,
                const std::shared_ptr<sw::redis::Redis> &redis_client,
                const ServiceManager::ptr &channels,
                const Discovery::ptr &service_discoverer,
                const std::string user_service_name,
                const std::string file_service_name,
                const std::string speech_service_name,
                const std::string message_service_name,
                const std::string transmite_service_name,
                const std::string friend_service_name)
                :_redis_session(std::make_shared<session>(redis_client)),
                _redis_status(std::make_shared<status>(redis_client)),
                _mm_channels(channels),
                _service_discoverer(service_discoverer),
                _user_service_name(user_service_name),
                _file_service_name(file_service_name),
                _speech_service_name(speech_service_name),
                _message_service_name(message_service_name),
                _transmite_service_name(transmite_service_name),
                _friend_service_name(friend_service_name),
                _connections(std::make_shared<Connection>()
        )
        {
            _ws_server.set_access_channels(websocketpp::log::alevel::none);
            _ws_server.init_asio();
            _ws_server.set_open_handler(std::bind(&GatewayServer::onOpen, this, std::placeholders::_1));
            _ws_server.set_close_handler(std::bind(&GatewayServer::onClose, this, std::placeholders::_1));
            auto wscb = std::bind(&GatewayServer::onMessage, this, 
                std::placeholders::_1, std::placeholders::_2);
            _ws_server.set_message_handler(wscb);
            _ws_server.set_reuse_addr(true);
            _ws_server.listen(websocket_port);
            _ws_server.start_accept();
            _http_server.Post(GET_PHONE_VERIFY_CODE  , (httplib::Server::Handler)std::bind(&GatewayServer::GetPhoneVerifyCode         , this, std::placeholders::_1, std::placeholders::_2));
            _http_server.Post(USERNAME_REGISTER      , (httplib::Server::Handler)std::bind(&GatewayServer::UserRegister               , this, std::placeholders::_1, std::placeholders::_2));
            _http_server.Post(USERNAME_LOGIN         , (httplib::Server::Handler)std::bind(&GatewayServer::UserLogin                  , this, std::placeholders::_1, std::placeholders::_2));
            _http_server.Post(PHONE_REGISTER         , (httplib::Server::Handler)std::bind(&GatewayServer::PhoneRegister              , this, std::placeholders::_1, std::placeholders::_2));
            _http_server.Post(PHONE_LOGIN            , (httplib::Server::Handler)std::bind(&GatewayServer::PhoneLogin                 , this, std::placeholders::_1, std::placeholders::_2));
            _http_server.Post(GET_USERINFO           , (httplib::Server::Handler)std::bind(&GatewayServer::GetUserInfo                , this, std::placeholders::_1, std::placeholders::_2));
            _http_server.Post(SET_USER_AVATAR        , (httplib::Server::Handler)std::bind(&GatewayServer::SetUserAvatar              , this, std::placeholders::_1, std::placeholders::_2));
            _http_server.Post(SET_USER_NICKNAME      , (httplib::Server::Handler)std::bind(&GatewayServer::SetUserNickname            , this, std::placeholders::_1, std::placeholders::_2));
            _http_server.Post(SET_USER_DESC          , (httplib::Server::Handler)std::bind(&GatewayServer::SetUserDescription         , this, std::placeholders::_1, std::placeholders::_2));
            _http_server.Post(SET_USER_PHONE         , (httplib::Server::Handler)std::bind(&GatewayServer::SetUserPhoneNumber         , this, std::placeholders::_1, std::placeholders::_2));
            _http_server.Post(FRIEND_GET_LIST        , (httplib::Server::Handler)std::bind(&GatewayServer::GetFriendList              , this, std::placeholders::_1, std::placeholders::_2));
            _http_server.Post(FRIEND_APPLY           , (httplib::Server::Handler)std::bind(&GatewayServer::FriendAdd                  , this, std::placeholders::_1, std::placeholders::_2));
            _http_server.Post(FRIEND_APPLY_PROCESS   , (httplib::Server::Handler)std::bind(&GatewayServer::FriendAddProcess           , this, std::placeholders::_1, std::placeholders::_2));
            _http_server.Post(FRIEND_REMOVE          , (httplib::Server::Handler)std::bind(&GatewayServer::FriendRemove               , this, std::placeholders::_1, std::placeholders::_2));
            _http_server.Post(FRIEND_SEARCH          , (httplib::Server::Handler)std::bind(&GatewayServer::FriendSearch               , this, std::placeholders::_1, std::placeholders::_2));
            _http_server.Post(FRIEND_GET_PENDING_EV  , (httplib::Server::Handler)std::bind(&GatewayServer::GetPendingFriendEventList  , this, std::placeholders::_1, std::placeholders::_2));
            _http_server.Post(CSS_GET_LIST           , (httplib::Server::Handler)std::bind(&GatewayServer::GetChatSessionList         , this, std::placeholders::_1, std::placeholders::_2));
            _http_server.Post(CSS_CREATE             , (httplib::Server::Handler)std::bind(&GatewayServer::ChatSessionCreate          , this, std::placeholders::_1, std::placeholders::_2));
            _http_server.Post(CSS_GET_MEMBER         , (httplib::Server::Handler)std::bind(&GatewayServer::GetChatSessionMember       , this, std::placeholders::_1, std::placeholders::_2));
            _http_server.Post(MSG_GET_RANGE          , (httplib::Server::Handler)std::bind(&GatewayServer::GetHistoryMsg              , this, std::placeholders::_1, std::placeholders::_2));
            _http_server.Post(MSG_GET_RECENT         , (httplib::Server::Handler)std::bind(&GatewayServer::GetRecentMsg               , this, std::placeholders::_1, std::placeholders::_2));
            _http_server.Post(MSG_KEY_SEARCH         , (httplib::Server::Handler)std::bind(&GatewayServer::MsgSearch                  , this, std::placeholders::_1, std::placeholders::_2));
            _http_server.Post(NEW_MESSAGE            , (httplib::Server::Handler)std::bind(&GatewayServer::NewMessage                 , this, std::placeholders::_1, std::placeholders::_2));
            _http_server.Post(FILE_GET_SINGLE        , (httplib::Server::Handler)std::bind(&GatewayServer::GetSingleFile              , this, std::placeholders::_1, std::placeholders::_2));
            _http_server.Post(FILE_GET_MULTI         , (httplib::Server::Handler)std::bind(&GatewayServer::GetMultiFile               , this, std::placeholders::_1, std::placeholders::_2));
            _http_server.Post(FILE_PUT_SINGLE        , (httplib::Server::Handler)std::bind(&GatewayServer::PutSingleFile              , this, std::placeholders::_1, std::placeholders::_2));
            _http_server.Post(FILE_PUT_MULTI         , (httplib::Server::Handler)std::bind(&GatewayServer::PutMultiFile               , this, std::placeholders::_1, std::placeholders::_2));
            _http_server.Post(SPEECH_RECOGNITION     , (httplib::Server::Handler)std::bind(&GatewayServer::SpeechRecognition          , this, std::placeholders::_1, std::placeholders::_2));
            _http_thread = std::thread([this, http_port](){
                _http_server.listen("0.0.0.0",http_port);
            });
            _http_thread.detach();
        }
        void start()
        {
            _ws_server.run();
        }
        ~GatewayServer() = default;
    private:
        void onOpen(websocketpp::connection_hdl hdl)
        {
            LOG_DEBUG("长连接打开");
        }
        void onClose(websocketpp::connection_hdl hdl)
        {
            //长连接断开时清理工作
            //移除登录会话信息
            auto conn = _ws_server.get_con_from_hdl(hdl);
            std::string uid,ssid;
            bool ret = _connections->client(conn,uid,ssid);
            if(!ret)
            {
                LOG_ERROR("长连接断开");
                return;
            }
            _redis_session->remove(ssid);
            //移除登录状态信息
            _redis_status->remove(uid);
            //移除长连接管理对象
            _connections->remove(conn);
            LOG_DEBUG("{} {} {} 长连接断开，清理缓存数据!", ssid, uid, (size_t)conn.get());
        }
        void onMessage(websocketpp::connection_hdl hdl, server_t::message_ptr msg)
        {
            //取出第一条消息后，根据消息会话中的会话id进行识别，将长连接客户端管理起来
            //取出长连接对应的管理对象
            auto conn = _ws_server.get_con_from_hdl(hdl);
            //针对消息进行反序列化
            ClientAuthenticationReq request;
            bool ret = request.ParseFromString(msg->get_payload());
            if(!ret)
            {
                LOG_ERROR("反序列化消息失败");
                _ws_server.close(hdl,websocketpp::close::status::unsupported_data,"反序列化消息失败");
                return;
            }
            //在会话消息缓存中查找信息
            std::string ssid = request.session_id();
            auto uid = _redis_session->uid(ssid);
            if(!uid)
            {
                LOG_ERROR("会话{}未注册",ssid);
                _ws_server.close(hdl,websocketpp::close::status::unsupported_data,"会话未注册");
                return;
            }
            //不存在关闭链接
            _connections->insert(conn,*uid,ssid);
            LOG_DEBUG("新增用户长连接信息：{}-{}-{}",(size_t)conn.get(),*uid,ssid);
            //存在则添加长连接管理
        }
        void GetPhoneVerifyCode(const httplib::Request& request,httplib::Response &response)
        {
            //从请求中取出正文信息
            PhoneVerifyCodeReq req;
            PhoneVerifyCodeRsp rsp;
            auto error_response = [&req,&rsp,&response] (const std::string& errmsg) ->void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
            };
            bool ret = req.ParseFromString(request.body);
            if(!ret)
            {
                LOG_ERROR("请求短信验证码反序列化消息失败");
                error_response("请求短信验证码反序列化消息失败");
                return;
            }
            //将请求转发给用户子服务
            auto channel = _mm_channels->choose(_user_service_name);
            if(!channel)
            {
                LOG_ERROR("未找到可提供服务的用户子服务节点{}", req.request_id());
                error_response("未找到可提供服务的用户子服务节点");
                return;
            }
            brpc::Controller cntl;
            yu::UserService_Stub stub(channel.get());
            stub.GetPhoneVerifyCode(&cntl,&req,&rsp,nullptr);
            if(cntl.Failed())
            {
                LOG_ERROR("请求短信验证码失败{}", req.request_id());
                error_response("请求短信验证码失败");
                return;
            }
            //得到响应，将响应进行序列化
            response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
        } 
        void UserRegister   (const httplib::Request& request,httplib::Response &response)
        {
            UserRegisterReq req;
            UserRegisterRsp rsp;
            auto error_response = [&req,&rsp,&response] (const std::string& errmsg) ->void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
            };
            bool ret = req.ParseFromString(request.body);
            if(!ret)
            {
                LOG_ERROR("用户注册反序列化消息失败",req.request_id());
                error_response("用户注册反序列化消息失败");
                return;
            }
            //将请求转发给用户子服务
            auto channel = _mm_channels->choose(_user_service_name);
            if(!channel)
            {
                LOG_ERROR("未找到可提供服务的用户子服务节点",req.request_id());
                error_response("未找到可提供服务的用户子服务节点");
                return;
            }
            brpc::Controller cntl;
            yu::UserService_Stub stub(channel.get());
            stub.UserRegister(&cntl,&req,&rsp,nullptr);
            if(cntl.Failed())
            {
                LOG_ERROR("用户注册验证码失败",req.request_id());
                error_response("用户注册验证码失败");
                return;
            }
            //得到响应，将响应进行序列化
            response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
        }            
        void UserLogin    (const httplib::Request& request,httplib::Response &response)
        {
            UserLoginReq req;
            UserLoginRsp rsp;
            auto error_response = [&req,&rsp,&response] (const std::string& errmsg) ->void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
            };
            bool ret = req.ParseFromString(request.body);
            if(!ret)
            {
                LOG_ERROR("用户登录反序列化消息失败",req.request_id());
                error_response("用户登录反序列化消息失败");
                return;
            }
            //将请求转发给用户子服务
            auto channel = _mm_channels->choose(_user_service_name);
            if(!channel)
            {
                LOG_ERROR("未找到可提供服务的用户子服务节点",req.request_id());
                error_response("未找到可提供服务的用户子服务节点");
                return;
            }
            brpc::Controller cntl;
            yu::UserService_Stub stub(channel.get());
            stub.UserLogin(&cntl,&req,&rsp,nullptr);
            if(cntl.Failed())
            {
                LOG_ERROR("用户注册验证码失败",req.request_id());
                error_response("用户注册验证码失败");
                return;
            }
            //得到响应，将响应进行序列化
            response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
        }               
        void PhoneRegister       (const httplib::Request& request,httplib::Response &response)
        {
            PhoneRegisterReq req;
            PhoneRegisterRsp rsp;
            auto error_response = [&req,&rsp,&response] (const std::string& errmsg) ->void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
            };
            bool ret = req.ParseFromString(request.body);
            if(!ret)
            {
                LOG_ERROR("用户注册反序列化消息失败",req.request_id());
                error_response("用户注册反序列化消息失败");
                return;
            }
            //将请求转发给用户子服务
            auto channel = _mm_channels->choose(_user_service_name);
            if(!channel)
            {
                LOG_ERROR("未找到可提供服务的用户子服务节点",req.request_id());
                error_response("未找到可提供服务的用户子服务节点");
                return;
            }
            brpc::Controller cntl;
            yu::UserService_Stub stub(channel.get());
            stub.PhoneRegister(&cntl,&req,&rsp,nullptr);
            if(cntl.Failed())
            {
                LOG_ERROR("用户注册验证码失败",req.request_id());
                error_response("用户注册验证码失败");
                return;
            }
            //得到响应，将响应进行序列化
            response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
        }        
        void PhoneLogin       (const httplib::Request& request,httplib::Response &response)
        {
            PhoneLoginReq req;
            PhoneLoginRsp rsp;
            auto error_response = [&req,&rsp,&response] (const std::string& errmsg) ->void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
            };
            bool ret = req.ParseFromString(request.body);
            if(!ret)
            {
                LOG_ERROR("用户注册反序列化消息失败",req.request_id());
                error_response("用户注册反序列化消息失败");
                return;
            }
            //将请求转发给用户子服务
            auto channel = _mm_channels->choose(_user_service_name);
            if(!channel)
            {
                LOG_ERROR("未找到可提供服务的用户子服务节点",req.request_id());
                error_response("未找到可提供服务的用户子服务节点");
                return;
            }
            brpc::Controller cntl;
            yu::UserService_Stub stub(channel.get());
            stub.PhoneLogin(&cntl,&req,&rsp,nullptr);
            if(cntl.Failed())
            {
                LOG_ERROR("用户登录验证码失败",req.request_id());
                error_response("用户登录验证码失败");
                return;
            }
            //得到响应，将响应进行序列化
            response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
        }           
        void GetUserInfo    (const httplib::Request& request,httplib::Response &response)
        {
            GetUserInfoReq req;
            GetUserInfoRsp rsp;
            auto error_response = [&req,&rsp,&response] (const std::string& errmsg) ->void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
            };
            bool ret = req.ParseFromString(request.body);
            if(!ret)
            {
                LOG_ERROR("获取用户信息反序列化消息失败",req.request_id());
                error_response("获取用户信息反序列化消息失败");
                return;
            }
            //进行鉴权
            std::string ssid = req.session_id();
            auto uid = _redis_session->uid(ssid);
            if(!uid)
            {
                LOG_ERROR("{}-获取登录会话关联用户信息失败！",ssid);
                error_response("获取登录会话关联用户信息失败！");
                return;
            }
            req.set_user_id(*uid);
            //将请求转发给用户子服务
            auto channel = _mm_channels->choose(_user_service_name);
            if(!channel)
            {
                LOG_ERROR("未找到可提供服务的用户子服务节点",req.request_id());
                error_response("未找到可提供服务的用户子服务节点");
                return;
            }
            brpc::Controller cntl;
            yu::UserService_Stub stub(channel.get());
            stub.GetUserInfo(&cntl,&req,&rsp,nullptr);
            if(cntl.Failed())
            {
                LOG_ERROR("用户子服务调用失败",req.request_id());
                error_response("用户子服务调用失败");
                return;
            }
            //得到响应，将响应进行序列化
            response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
        }             
        void SetUserAvatar  (const httplib::Request& request,httplib::Response &response)
        {
            SetUserAvatarReq req;
            SetUserAvatarRsp rsp;
            auto error_response = [&req,&rsp,&response] (const std::string& errmsg) ->void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
            };
            bool ret = req.ParseFromString(request.body);
            if(!ret)
            {
                LOG_ERROR("用户头像设置反序列化消息失败",req.request_id());
                error_response("用户头像设置反序列化消息失败");
                return;
            }
            //进行鉴权
            std::string ssid = req.session_id();
            auto uid = _redis_session->uid(ssid);
            if(!uid)
            {
                LOG_ERROR("{}-获取登录会话关联用户信息失败！",ssid);
                error_response("获取登录会话关联用户信息失败！");
                return;
            }
            req.set_user_id(*uid);
            //将请求转发给用户子服务
            auto channel = _mm_channels->choose(_user_service_name);
            if(!channel)
            {
                LOG_ERROR("未找到可提供服务的用户子服务节点",req.request_id());
                error_response("未找到可提供服务的用户子服务节点");
                return;
            }
            brpc::Controller cntl;
            yu::UserService_Stub stub(channel.get());
            stub.SetUserAvatar(&cntl,&req,&rsp,nullptr);
            if(cntl.Failed())
            {
                LOG_ERROR("用户子服务调用失败",req.request_id());
                error_response("用户子服务调用失败");
                return;
            }
            //得到响应，将响应进行序列化
            response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
        }             
        void SetUserNickname   (const httplib::Request& request,httplib::Response &response)
        {
            SetUserNicknameReq req;
            SetUserNicknameRsp rsp;
            auto error_response = [&req,&rsp,&response] (const std::string& errmsg) ->void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
            };
            bool ret = req.ParseFromString(request.body);
            if(!ret)
            {
                LOG_ERROR("用户昵称设置反序列化消息失败",req.request_id());
                error_response("用户昵称设置反序列化消息失败");
                return;
            }
            //进行鉴权
            std::string ssid = req.session_id();
            auto uid = _redis_session->uid(ssid);
            if(!uid)
            {
                LOG_ERROR("{}-获取登录会话关联用户信息失败！",ssid);
                error_response("获取登录会话关联用户信息失败！");
                return;
            }
            req.set_user_id(*uid);
            //将请求转发给用户子服务
            auto channel = _mm_channels->choose(_user_service_name);
            if(!channel)
            {
                LOG_ERROR("未找到可提供服务的用户子服务节点",req.request_id());
                error_response("未找到可提供服务的用户子服务节点");
                return;
            }
            brpc::Controller cntl;
            yu::UserService_Stub stub(channel.get());
            stub.SetUserNickname(&cntl,&req,&rsp,nullptr);
            if(cntl.Failed())
            {
                LOG_ERROR("用户子服务调用失败",req.request_id());
                error_response("用户子服务调用失败");
                return;
            }
            //得到响应，将响应进行序列化
            response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
        }          
        void SetUserDescription (const httplib::Request& request,httplib::Response &response)
        {
            SetUserDescriptionReq req;
            SetUserDescriptionRsp rsp;
            auto error_response = [&req,&rsp,&response] (const std::string& errmsg) ->void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
            };
            bool ret = req.ParseFromString(request.body);
            if(!ret)
            {
                LOG_ERROR("用户签名设置反序列化消息失败",req.request_id());
                error_response("用户签名设置反序列化消息失败");
                return;
            }
            //进行鉴权
            std::string ssid = req.session_id();
            auto uid = _redis_session->uid(ssid);
            if(!uid)
            {
                LOG_ERROR("{}-获取登录会话关联用户信息失败！",ssid);
                error_response("获取登录会话关联用户信息失败！");
                return;
            }
            req.set_user_id(*uid);
            //将请求转发给用户子服务
            auto channel = _mm_channels->choose(_user_service_name);
            if(!channel)
            {
                LOG_ERROR("未找到可提供服务的用户子服务节点",req.request_id());
                error_response("未找到可提供服务的用户子服务节点");
                return;
            }
            brpc::Controller cntl;
            yu::UserService_Stub stub(channel.get());
            stub.SetUserDescription(&cntl,&req,&rsp,nullptr);
            if(cntl.Failed())
            {
                LOG_ERROR("好友子服务调用失败",req.request_id());
                error_response("好友子服务调用失败");
                return;
            }
            //得到响应，将响应进行序列化
            response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
        }         
        void SetUserPhoneNumber  (const httplib::Request& request,httplib::Response &response)
        {
            SetUserPhoneNumberReq req;
            SetUserPhoneNumberRsp rsp;
            auto error_response = [&req,&rsp,&response] (const std::string& errmsg) ->void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
            };
            bool ret = req.ParseFromString(request.body);
            if(!ret)
            {
                LOG_ERROR("手机号设置反序列化消息失败",req.request_id());
                error_response("手机号设置反序列化消息失败");
                return;
            }
            //进行鉴权
            std::string ssid = req.session_id();
            auto uid = _redis_session->uid(ssid);
            if(!uid)
            {
                LOG_ERROR("{}-获取登录会话关联用户信息失败！",ssid);
                error_response("获取登录会话关联用户信息失败！");
                return;
            }
            req.set_user_id(*uid);
            //将请求转发给用户子服务
            auto channel = _mm_channels->choose(_user_service_name);
            if(!channel)
            {
                LOG_ERROR("未找到可提供服务的用户子服务节点",req.request_id());
                error_response("未找到可提供服务的用户子服务节点");
                return;
            }
            brpc::Controller cntl;
            yu::UserService_Stub stub(channel.get());
            stub.SetUserPhoneNumber(&cntl,&req,&rsp,nullptr);
            if(cntl.Failed())
            {
                LOG_ERROR("好友子服务调用失败",req.request_id());
                error_response("好友子服务调用失败");
                return;
            }
            //得到响应，将响应进行序列化
            response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
        }        
        void GetFriendList   (const httplib::Request& request,httplib::Response &response)
        {
            GetFriendListReq req;
            GetFriendListRsp rsp;
            auto error_response = [&req,&rsp,&response] (const std::string& errmsg) ->void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
            };
            bool ret = req.ParseFromString(request.body);
            if(!ret)
            {
                LOG_ERROR("获取好友列表反序列化消息失败",req.request_id());
                error_response("获取好友列表反序列化消息失败");
                return;
            }
            //进行鉴权
            std::string ssid = req.session_id();
            auto uid = _redis_session->uid(ssid);
            if(!uid)
            {
                LOG_ERROR("{}-获取登录会话关联用户信息失败！",ssid);
                error_response("获取登录会话关联用户信息失败！");
                return;
            }
            req.set_user_id(*uid);
            //将请求转发给用户子服务
            auto channel = _mm_channels->choose(_user_service_name);
            if(!channel)
            {
                LOG_ERROR("未找到可提供服务的用户子服务节点",req.request_id());
                error_response("未找到可提供服务的用户子服务节点");
                return;
            }
            brpc::Controller cntl;
            yu::FriendService_Stub stub(channel.get());
            stub.GetFriendList(&cntl,&req,&rsp,nullptr);
            if(cntl.Failed())
            {
                LOG_ERROR("好友子服务调用失败",req.request_id());
                error_response("好友子服务调用失败");
                return;
            }
            //得到响应，将响应进行序列化
            response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
        }         
        std::shared_ptr<GetUserInfoRsp>   _GetUserInfo(const std::string &rid,const std::string &uid)
        {
            GetUserInfoReq req;
            auto rsp = std::make_shared<GetUserInfoRsp>();
            req.set_user_id(uid);
            req.set_request_id(rid);
            auto channel = _mm_channels->choose(_user_service_name);
            if(!channel)
            {
                LOG_ERROR("未找到可提供服务的用户子服务节点",req.request_id());
                return std::shared_ptr<GetUserInfoRsp>();
            }
            brpc::Controller cntl;
            yu::UserService_Stub stub(channel.get());
            stub.GetUserInfo(&cntl,&req,rsp.get(),nullptr);
            if(cntl.Failed())
            {
                LOG_ERROR("用户子服务调用失败",rid);
                return std::shared_ptr<GetUserInfoRsp>();
            }
            return rsp;
        }
        void FriendAdd      (const httplib::Request& request,httplib::Response &response)
        {
            //好友申请的处理中好友子服务只是在数据库中创建了申请事件
            //网关要做的就是当好友子服务将业务处理完成后如果处理是成功的需要通知被申请方
            //1、正文的反序列化，提取登录会话id
            FriendAddReq req;
            FriendAddRsp rsp;
            auto error_response = [&req,&rsp,&response] (const std::string& errmsg) ->void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
            };
            bool ret = req.ParseFromString(request.body);
            if(!ret)
            {
                LOG_ERROR("好友申请反序列化消息失败",req.request_id());
                error_response("好友申请反序列化消息失败");
                return;
            }
            //2、客户端身份识别与鉴权
            std::string ssid = req.session_id();
            auto uid = _redis_session->uid(ssid);
            if(!uid)
            {
                LOG_ERROR("{}-获取登录会话关联用户信息失败！",ssid);
                error_response("获取登录会话关联用户信息失败！");
                return;
            }
            req.set_user_id(*uid);
            //3、将请求转发给好友子服务
             auto channel = _mm_channels->choose(_friend_service_name);
            if(!channel)
            {
                LOG_ERROR("未找到可提供服务的好友子服务节点",req.request_id());
                error_response("未找到可提供服务的好友子服务节点");
                return;
            }
            brpc::Controller cntl;
            yu::FriendService_Stub stub(channel.get());
            stub.FriendAdd(&cntl,&req,&rsp,nullptr);
            if(cntl.Failed())
            {
                LOG_ERROR("好友子服务调用失败",req.request_id());
                error_response("好友子服务调用失败");
                return;
            }
            //4、若处理成功且获取被申请方用户长链接成功则需要通知被申请方
            auto conn = _connections->connection(*uid);
            if(rsp.success()&&conn)
            {
                NotifyMessage notify;
                auto user_rsp = _GetUserInfo(rsp.request_id(),*uid);
                if(!user_rsp)
                {
                    LOG_ERROR("获取当前用户信息失败 uid={}", *uid);
                    return error_response("获取当前用户信息失败");
                }
                notify.set_notify_type(NotifyType::FRIEND_ADD_APPLY_NOTIFY);
                notify.mutable_friend_add_apply()->mutable_user_info()->CopyFrom(user_rsp->user_info());
                conn->send(notify.SerializeAsString(),websocketpp::frame::opcode::value::binary);
            }
            response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
        }             
        void FriendAddProcess     (const httplib::Request& request,httplib::Response &response)
        {
            FriendAddProcessReq req;
            FriendAddProcessRsp rsp;
            auto error_response = [&req,&rsp,&response] (const std::string& errmsg) ->void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
            };
            bool ret = req.ParseFromString(request.body);
            if(!ret)
            {
                LOG_ERROR("好友申请处理反序列化消息失败",req.request_id());
                error_response("好友申请处理反序列化消息失败");
                return;
            }
            //2、客户端身份识别与鉴权
            std::string ssid = req.session_id();
            auto uid = _redis_session->uid(ssid);
            if(!uid)
            {
                LOG_ERROR("{}-获取登录会话关联用户信息失败！",ssid);
                error_response("获取登录会话关联用户信息失败！");
                return;
            }
            req.set_user_id(*uid);
            //3、将请求转发给好友子服务
             auto channel = _mm_channels->choose(_friend_service_name);
            if(!channel)
            {
                LOG_ERROR("未找到可提供服务的好友子服务节点",req.request_id());
                error_response("未找到可提供服务的好友子服务节点");
                return;
            }
            brpc::Controller cntl;
            yu::FriendService_Stub stub(channel.get());
            stub.FriendAddProcess(&cntl,&req,&rsp,nullptr);
            if(cntl.Failed())
            {
                LOG_ERROR("好友子服务调用失败",req.request_id());
                error_response("好友子服务调用失败");
                return;
            }
            if(rsp.success())
            {
                auto process_user_rsp = _GetUserInfo(rsp.request_id(),*uid);
                if(!process_user_rsp)
                {
                    LOG_ERROR("获取当前用户信息失败 uid={}", *uid);
                    return error_response("获取当前用户信息失败");
                }
                auto apply_user_rsp = _GetUserInfo(rsp.request_id(),req.apply_user_id());
                if(!apply_user_rsp)
                {
                    LOG_ERROR("获取被申请方用户信息失败",req.apply_user_id());
                    return error_response("获取被申请方用户信息失败");
                }
                auto process_conn = _connections->connection(*uid);
                if(!process_conn)
                {
                    LOG_ERROR("用户{}未连接",*uid);
                    return error_response("用户未连接");
                }
                auto apply_conn = _connections->connection(req.apply_user_id());
                if(!apply_conn)
                {
                    LOG_ERROR("用户{}未连接",req.apply_user_id());
                    return error_response("用户未连接");
                }
                //4. 将处理结果给申请人进行通知
                if(apply_conn)
                {
                    NotifyMessage notify;
                    notify.set_notify_type(NotifyType::FRIEND_ADD_PROCESS_NOTIFY);
                    notify.mutable_friend_process_result()->set_agree(req.agree());
                    notify.mutable_friend_process_result()->mutable_user_info()->CopyFrom(process_user_rsp->user_info());
                    apply_conn->send(notify.SerializeAsString(),websocketpp::frame::opcode::value::binary);
                }
                //5. 若处理结果是同意 --- 会伴随着单聊会话的创建 -- 因此需要对双方进行会话创建的通知
                if(req.agree()&&apply_conn)//通知申请人
                {
                    NotifyMessage notify;
                    notify.set_notify_type(NotifyType::CHAT_SESSION_CREATE_NOTIFY);
                    auto chat_session = notify.mutable_new_chat_session_info();
                    chat_session->mutable_chat_session_info()->set_single_chat_friend_id(*uid);
                    chat_session->mutable_chat_session_info()->set_chat_session_id(rsp.new_session_id());
                    chat_session->mutable_chat_session_info()->set_chat_session_name(process_user_rsp->user_info().nickname());
                    chat_session->mutable_chat_session_info()->set_avatar(process_user_rsp->user_info().avatar());
                    apply_conn->send(notify.SerializeAsString(),websocketpp::frame::opcode::value::binary);
                }
                if(req.agree()&&process_conn)//通知被申请方
                {
                    NotifyMessage notify;
                    notify.set_notify_type(NotifyType::CHAT_SESSION_CREATE_NOTIFY);
                    auto chat_session = notify.mutable_new_chat_session_info();
                    chat_session->mutable_chat_session_info()->set_single_chat_friend_id(req.apply_user_id());
                    chat_session->mutable_chat_session_info()->set_chat_session_id(rsp.new_session_id());
                    chat_session->mutable_chat_session_info()->set_chat_session_name(apply_user_rsp->user_info().nickname());
                    chat_session->mutable_chat_session_info()->set_avatar(apply_user_rsp->user_info().avatar());
                    process_conn->send(notify.SerializeAsString(),websocketpp::frame::opcode::value::binary);
                }
            }
            response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
        }       
        void FriendRemove    (const httplib::Request& request,httplib::Response &response)
        {
            //1、正文的反序列化，提取登录会话id
            FriendRemoveReq req;
            FriendRemoveRsp rsp;
            auto error_response = [&req,&rsp,&response] (const std::string& errmsg) ->void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
            };
            bool ret = req.ParseFromString(request.body);
            if(!ret)
            {
                LOG_ERROR("好友删除反序列化消息失败",req.request_id());
                error_response("好友删除反序列化消息失败");
                return;
            }
            //2、客户端身份识别与鉴权
            std::string ssid = req.session_id();
            auto uid = _redis_session->uid(ssid);
            if(!uid)
            {
                LOG_ERROR("{}-获取登录会话关联用户信息失败！",ssid);
                error_response("获取登录会话关联用户信息失败！");
                return;
            }
            req.set_user_id(*uid);
            //3、将请求转发给好友子服务
             auto channel = _mm_channels->choose(_friend_service_name);
            if(!channel)
            {
                LOG_ERROR("未找到可提供服务的好友子服务节点",req.request_id());
                error_response("未找到可提供服务的好友子服务节点");
                return;
            }
            brpc::Controller cntl;
            yu::FriendService_Stub stub(channel.get());
            stub.FriendRemove(&cntl,&req,&rsp,nullptr);
            if(cntl.Failed())
            {
                LOG_ERROR("好友子服务调用失败",req.request_id());
                error_response("好友子服务调用失败");
                return;
            }
            //4、若处理成功且获取被申请方用户长链接成功则需要通知被申请方
            auto conn = _connections->connection(req.peer_id());
            if(rsp.success()&&conn)
            {
                NotifyMessage notify;
                auto user_rsp = _GetUserInfo(rsp.request_id(),*uid);
                if(!user_rsp)
                {
                    LOG_ERROR("获取当前用户信息失败",*uid);
                    return error_response("获取当前用户信息失败");
                }
                notify.set_notify_type(NotifyType::FRIEND_REMOVE_NOTIFY);
                notify.mutable_friend_remove()->set_user_id(*uid);
                conn->send(notify.SerializeAsString(),websocketpp::frame::opcode::value::binary);
            }
            response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
        }            
        void FriendSearch       (const httplib::Request& request,httplib::Response &response)
        {
            //1、正文的反序列化，提取登录会话id
            FriendSearchReq req;
            FriendSearchRsp rsp;
            auto error_response = [&req,&rsp,&response] (const std::string& errmsg) ->void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
            };
            bool ret = req.ParseFromString(request.body);
            if(!ret)
            {
                LOG_ERROR("好友搜索反序列化消息失败",req.request_id());
                error_response("好友搜索反序列化消息失败");
                return;
            }
            //2、客户端身份识别与鉴权
            std::string ssid = req.session_id();
            auto uid = _redis_session->uid(ssid);
            if(!uid)
            {
                LOG_ERROR("{}-获取登录会话关联用户信息失败！",ssid);
                error_response("获取登录会话关联用户信息失败！");
                return;
            }
            req.set_user_id(*uid);
            //3、将请求转发给好友子服务
             auto channel = _mm_channels->choose(_friend_service_name);
            if(!channel)
            {
                LOG_ERROR("未找到可提供服务的好友子服务节点",req.request_id());
                error_response("未找到可提供服务的好友子服务节点");
                return;
            }
            brpc::Controller cntl;
            yu::FriendService_Stub stub(channel.get());
            stub.FriendSearch(&cntl,&req,&rsp,nullptr);
            if(cntl.Failed())
            {
                LOG_ERROR("好友子服务调用失败",req.request_id());
                error_response("好友子服务调用失败");
                return;
            }
            response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
        }         
        void GetPendingFriendEventList  (const httplib::Request& request,httplib::Response &response)
        {
          //1、正文的反序列化，提取登录会话id
            GetPendingFriendEventListReq req;
            GetPendingFriendEventListRsp rsp;
            auto error_response = [&req,&rsp,&response] (const std::string& errmsg) ->void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(),"application/x-protobuf");
            };
            bool ret = req.ParseFromString(request.body);
            if(!ret)
            {
                LOG_ERROR("待处理好友列表反序列化消息失败",req.request_id());
                error_response("待处理好友列表反序列化消息失败");
                return;
            }
            //2、客户端身份识别与鉴权
            std::string ssid = req.session_id();
            auto uid = _redis_session->uid(ssid);
            if(!uid)
            {
                LOG_ERROR("{}-获取登录会话关联用户信息失败！",ssid);
                error_response("获取登录会话关联用户信息失败！");
                return;
            }
            req.set_user_id(*uid);
            //3、将请求转发给好友子服务
             auto channel = _mm_channels->choose(_friend_service_name);
            if(!channel)
            {
                LOG_ERROR("未找到可提供服务的好友子服务节点",req.request_id());
                error_response("未找到可提供服务的好友子服务节点");
                return;
            }
            brpc::Controller cntl;
            yu::FriendService_Stub stub(channel.get());
            stub.GetPendingFriendEventList(&cntl,&req,&rsp,nullptr);
            if(cntl.Failed())
            {
                LOG_ERROR("好友子服务调用失败",req.request_id());
                error_response("好友子服务调用失败");
                return;
            }
            response.set_content(rsp.SerializeAsString(),"application/x-protobuf");     
        } 
        void GetChatSessionList  (const httplib::Request& request,httplib::Response &response)
        {
            GetChatSessionListReq req;
            GetChatSessionListRsp rsp;
            auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            };
            bool ret = req.ParseFromString(request.body);
            if (ret == false) 
            {
                LOG_ERROR("获取聊天会话列表请求正文反序列化失败！");
                return err_response("获取聊天会话列表请求正文反序列化失败！");
            }
            // 2. 客户端身份识别与鉴权
            std::string ssid = req.session_id();
            auto uid = _redis_session->uid(ssid);
            if (!uid) 
            {
                LOG_ERROR("{} 获取登录会话关联用户信息失败！", ssid);
                return err_response("获取登录会话关联用户信息失败！");
            }
            req.set_user_id(*uid);
            // 3. 将请求转发给好友子服务进行业务处理
            auto channel = _mm_channels->choose(_friend_service_name);
            if (!channel) 
            {
                LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                return err_response("未找到可提供业务处理的用户子服务节点！");
            }
            yu::FriendService_Stub stub(channel.get());
            brpc::Controller cntl;
            stub.GetChatSessionList(&cntl, &req, &rsp, nullptr);
            if (cntl.Failed()) 
            {
                LOG_ERROR("{} 好友子服务调用失败！", req.request_id());
                return err_response("好友子服务调用失败！");
            }
            // 5. 向客户端进行响应
            response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
        }        
        void ChatSessionCreate    (const httplib::Request& request,httplib::Response &response)
        {
            ChatSessionCreateReq req;
            ChatSessionCreateRsp rsp;
            auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            };
            bool ret = req.ParseFromString(request.body);
            if (ret == false) 
            {
                LOG_ERROR("创建聊天会话请求正文反序列化失败！");
                return err_response("创建聊天会话请求正文反序列化失败！");
            }
            // 2. 客户端身份识别与鉴权
            std::string ssid = req.session_id();
            auto uid = _redis_session->uid(ssid);
            if (!uid) 
            {
                LOG_ERROR("{} 获取登录会话关联用户信息失败！", ssid);
                return err_response("获取登录会话关联用户信息失败！");
            }
            req.set_user_id(*uid);
            // 3. 将请求转发给好友子服务进行业务处理
            auto channel = _mm_channels->choose(_friend_service_name);
            if (!channel) 
            {
                LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                return err_response("未找到可提供业务处理的用户子服务节点！");
            }
            yu::FriendService_Stub stub(channel.get());
            brpc::Controller cntl;
            stub.ChatSessionCreate(&cntl, &req, &rsp, nullptr);
            if (cntl.Failed()) 
            {
                LOG_ERROR("{} 好友子服务调用失败！", req.request_id());
                return err_response("好友子服务调用失败！");
            }
            if(rsp.success())
            {
                for(int i = 0;i<req.member_id_list_size();i++)
                {
                    auto conn = _connections->connection(req.member_id_list(i));
                    if(!conn)
                    {
                        LOG_DEBUG("未找到群聊成员 {} 长连接", req.member_id_list(i));
                        continue;
                    }
                    NotifyMessage notify;
                    notify.set_notify_type(NotifyType::CHAT_SESSION_CREATE_NOTIFY);
                    notify.mutable_new_chat_session_info()->mutable_chat_session_info()->CopyFrom(rsp.chat_session_info());
                    conn->send(notify.SerializeAsString(),websocketpp::frame::opcode::value::binary);
                }

            }
            // 5. 向客户端进行响应
            response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
        }       
        void GetChatSessionMember (const httplib::Request& request,httplib::Response &response)
        {
            GetChatSessionMemberReq req;
            GetChatSessionMemberRsp rsp;
            auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            };
            bool ret = req.ParseFromString(request.body);
            if (ret == false) 
            {
                LOG_ERROR("获取聊天会话成员请求正文反序列化失败！");
                return err_response("获取聊天会话成员请求正文反序列化失败！");
            }
            // 2. 客户端身份识别与鉴权
            std::string ssid = req.session_id();
            auto uid = _redis_session->uid(ssid);
            if (!uid) 
            {
                LOG_ERROR("{} 获取登录会话关联用户信息失败！", ssid);
                return err_response("获取登录会话关联用户信息失败！");
            }
            req.set_user_id(*uid);
            // 3. 将请求转发给好友子服务进行业务处理
            auto channel = _mm_channels->choose(_friend_service_name);
            if (!channel) 
            {
                LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                return err_response("未找到可提供业务处理的用户子服务节点！");
            }
            yu::FriendService_Stub stub(channel.get());
            brpc::Controller cntl;
            stub.GetChatSessionMember(&cntl, &req, &rsp, nullptr);
            if (cntl.Failed()) 
            {
                LOG_ERROR("{} 好友子服务调用失败！", req.request_id());
                return err_response("好友子服务调用失败！");
            }
            // 5. 向客户端进行响应
            response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
        }  

        void GetHistoryMsg   (const httplib::Request& request,httplib::Response &response)
        {
            GetHistoryMsgReq req;
            GetHistoryMsgRsp rsp;
            auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            };
            bool ret = req.ParseFromString(request.body);
            if (ret == false) 
            {
                LOG_ERROR("获取历史消息请求正文反序列化失败！");
                return err_response("获取历史消息请求正文反序列化失败！");
            }
            // 2. 客户端身份识别与鉴权
            std::string ssid = req.session_id();
            auto uid = _redis_session->uid(ssid);
            if (!uid) 
            {
                LOG_ERROR("{} 获取登录会话关联用户信息失败！", ssid);
                return err_response("获取登录会话关联用户信息失败！");
            }
            req.set_user_id(*uid);
            // 3. 将请求转发给消息子服务进行业务处理
            auto channel = _mm_channels->choose(_message_service_name);
            if (!channel) 
            {
                LOG_ERROR("{} 未找到可提供业务处理的消息子服务节点！", req.request_id());
                return err_response("未找到可提供业务处理的消息子服务节点！");
            }
            yu::MsgStorageService_Stub stub(channel.get());
            brpc::Controller cntl;
            stub.GetHistoryMsg(&cntl, &req, &rsp, nullptr);
            if (cntl.Failed()) 
            {
                LOG_ERROR("{} 消息子服务调用失败！", req.request_id());
                return err_response("消息子服务调用失败！");
            }
            // 5. 向客户端进行响应
            response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
        }            
        void GetRecentMsg    (const httplib::Request& request,httplib::Response &response)
        {
            GetRecentMsgReq req;
            GetRecentMsgRsp rsp;
            auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            };
            bool ret = req.ParseFromString(request.body);
            if (ret == false) 
            {
                LOG_ERROR("获取最近消息请求正文反序列化失败！");
                return err_response("获取最近消息请求正文反序列化失败！");
            }
            // 2. 客户端身份识别与鉴权
            std::string ssid = req.session_id();
            auto uid = _redis_session->uid(ssid);
            if (!uid) 
            {
                LOG_ERROR("{} 获取登录会话关联用户信息失败！", ssid);
                return err_response("获取登录会话关联用户信息失败！");
            }
            req.set_user_id(*uid);
            // 3. 将请求转发给消息子服务进行业务处理
            auto channel = _mm_channels->choose(_message_service_name);
            if (!channel) 
            {
                LOG_ERROR("{} 未找到可提供业务处理的消息子服务节点！", req.request_id());
                return err_response("未找到可提供业务处理的消息子服务节点！");
            }
            yu::MsgStorageService_Stub stub(channel.get());
            brpc::Controller cntl;
            stub.GetRecentMsg(&cntl, &req, &rsp, nullptr);
            if (cntl.Failed()) 
            {
                LOG_ERROR("{} 消息子服务调用失败！", req.request_id());
                return err_response("消息子服务调用失败！");
            }
            // 5. 向客户端进行响应
            response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
        }            
        void MsgSearch     (const httplib::Request& request,httplib::Response &response)
        {
            MsgSearchReq req;
            MsgSearchRsp rsp;
            auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            };
            bool ret = req.ParseFromString(request.body);
            if (ret == false) 
            {
                LOG_ERROR("搜索消息请求正文反序列化失败！");
                return err_response("搜索消息请求正文反序列化失败！");
            }
            // 2. 客户端身份识别与鉴权
            std::string ssid = req.session_id();
            auto uid = _redis_session->uid(ssid);
            if (!uid) 
            {
                LOG_ERROR("{} 获取登录会话关联用户信息失败！", ssid);
                return err_response("获取登录会话关联用户信息失败！");
            }
            req.set_user_id(*uid);
            // 3. 将请求转发给消息子服务进行业务处理
            auto channel = _mm_channels->choose(_message_service_name);
            if (!channel) 
            {
                LOG_ERROR("{} 未找到可提供业务处理的消息子服务节点！", req.request_id());
                return err_response("未找到可提供业务处理的消息子服务节点！");
            }
            yu::MsgStorageService_Stub stub(channel.get());
            brpc::Controller cntl;
            stub.MsgSearch(&cntl, &req, &rsp, nullptr);
            if (cntl.Failed()) 
            {
                LOG_ERROR("{} 消息子服务调用失败！", req.request_id());
                return err_response("消息子服务调用失败！");
            }
            // 5. 向客户端进行响应
            response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
        }              
        void NewMessage        (const httplib::Request& request,httplib::Response &response)
        {
            NewMessageReq req;
            NewMessageRsp rsp;
            GetTransmitTargetRsp _traget_rsp;
            auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            };
            bool ret = req.ParseFromString(request.body);
            if (ret == false) 
            {
                LOG_ERROR("新消息请求正文反序列化失败！");
                return err_response("新消息请求正文反序列化失败！");
            }
            // 2. 客户端身份识别与鉴权
            std::string ssid = req.session_id();
            auto uid = _redis_session->uid(ssid);
            if (!uid) 
            {
                LOG_ERROR("{} 获取登录会话关联用户信息失败！", ssid);
                return err_response("获取登录会话关联用户信息失败！");
            }
            req.set_user_id(*uid);
            // 3. 将请求转发给消息转发子服务进行业务处理
            auto channel = _mm_channels->choose(_transmite_service_name);
            if (!channel) 
            {
                LOG_ERROR("{} 未找到可提供业务处理的消息转发子服务节点！", req.request_id());
                return err_response("未找到可提供业务处理的消息转发子服务节点！");
            }
            yu::MsgTransmitService_Stub stub(channel.get());
            brpc::Controller cntl;
            stub.GetTransmitTarget(&cntl, &req, &_traget_rsp, nullptr);
            if (cntl.Failed()) 
            {
                LOG_ERROR("{} 消息转发子服务调用失败！", req.request_id());
                return err_response("消息转发子服务调用失败！");
            }
            if(_traget_rsp.success())
            {
                for(int i = 0;i<_traget_rsp.target_id_list_size();i++)
                {
                    std::string notify_id = _traget_rsp.target_id_list(i);
                    if(notify_id == *uid) continue;
                    auto conn = _connections->connection(notify_id);
                    if(!conn)
                    {
                        continue;
                    }
                    NotifyMessage notify;
                    notify.set_notify_type(NotifyType::CHAT_MESSAGE_NOTIFY);
                    notify.mutable_new_message_info()->mutable_message_info()->CopyFrom(_traget_rsp.message());
                    conn->send(notify.SerializeAsString(),websocketpp::frame::opcode::value::binary);
                }
            }
            // 5. 向客户端进行响应
            rsp.set_request_id(req.request_id());
            rsp.set_success(_traget_rsp.success());
            rsp.set_errmsg(_traget_rsp.errmsg());
            response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
        }          
        void GetSingleFile   (const httplib::Request& request,httplib::Response &response)
        {
            GetSingleFileReq req;
            GetSingleFileRsp rsp;
            auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            };
            bool ret = req.ParseFromString(request.body);
            if (ret == false) 
            {
                LOG_ERROR("获取单个文件请求正文反序列化失败！");
                return err_response("获取单个文件请求正文反序列化失败！");
            }
            // 2. 客户端身份识别与鉴权
            std::string ssid = req.session_id();
            auto uid = _redis_session->uid(ssid);
            if (!uid) 
            {
                LOG_ERROR("{} 获取登录会话关联用户信息失败！", ssid);
                return err_response("获取登录会话关联用户信息失败！");
            }
            req.set_user_id(*uid);
            // 3. 将请求转发给文件子服务进行业务处理
            auto channel = _mm_channels->choose(_file_service_name);
            if (!channel) 
            {
                LOG_ERROR("{} 未找到可提供业务处理的文件子服务节点！", req.request_id());
                return err_response("未找到可提供业务处理的文件子服务节点！");
            }
            yu::FileService_Stub stub(channel.get());
            brpc::Controller cntl;
            stub.GetSingleFile(&cntl, &req, &rsp, nullptr);
            if (cntl.Failed()) 
            {
                LOG_ERROR("{} 文件子服务调用失败！", req.request_id());
                return err_response("文件子服务调用失败！");
            }
            // 5. 向客户端进行响应
            response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
        }            
        void GetMultiFile     (const httplib::Request& request,httplib::Response &response)
        {
            GetMultiFileReq req;
            GetMultiFileRsp rsp;
            auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            };
            bool ret = req.ParseFromString(request.body);
            if (ret == false) 
            {
                LOG_ERROR("获取多个文件请求正文反序列化失败！");
                return err_response("获取多个文件请求正文反序列化失败！");
            }
            // 2. 客户端身份识别与鉴权
            std::string ssid = req.session_id();
            auto uid = _redis_session->uid(ssid);
            if (!uid) 
            {
                LOG_ERROR("{} 获取登录会话关联用户信息失败！", ssid);
                return err_response("获取登录会话关联用户信息失败！");
            }
            req.set_user_id(*uid);
            // 3. 将请求转发给文件子服务进行业务处理
            auto channel = _mm_channels->choose(_file_service_name);
            if (!channel) 
            {
                LOG_ERROR("{} 未找到可提供业务处理的文件子服务节点！", req.request_id());
                return err_response("未找到可提供业务处理的文件子服务节点！");
            }
            yu::FileService_Stub stub(channel.get());
            brpc::Controller cntl;
            stub.GetMultiFile(&cntl, &req, &rsp, nullptr);
            if (cntl.Failed()) 
            {
                LOG_ERROR("{} 文件子服务调用失败！", req.request_id());
                return err_response("文件子服务调用失败！");
            }
            // 5. 向客户端进行响应
            response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
        }           
        void PutSingleFile    (const httplib::Request& request,httplib::Response &response)
        {
            PutSingleFileReq req;
            PutSingleFileRsp rsp;
            auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            };
            bool ret = req.ParseFromString(request.body);
            if (ret == false) 
            {
                LOG_ERROR("上传单个文件请求正文反序列化失败！");
                return err_response("上传单个文件请求正文反序列化失败！");
            }
            // 2. 客户端身份识别与鉴权
            std::string ssid = req.session_id();
            auto uid = _redis_session->uid(ssid);
            if (!uid) 
            {
                LOG_ERROR("{} 获取登录会话关联用户信息失败！", ssid);
                return err_response("获取登录会话关联用户信息失败！");
            }
            req.set_user_id(*uid);
            // 3. 将请求转发给文件子服务进行业务处理
            auto channel = _mm_channels->choose(_file_service_name);
            if (!channel) 
            {
                LOG_ERROR("{} 未找到可提供业务处理的文件子服务节点！", req.request_id());
                return err_response("未找到可提供业务处理的文件子服务节点！");
            }
            yu::FileService_Stub stub(channel.get());
            brpc::Controller cntl;
            stub.PutSingleFile(&cntl, &req, &rsp, nullptr);
            if (cntl.Failed()) 
            {
                LOG_ERROR("{} 文件子服务调用失败！", req.request_id());
                return err_response("文件子服务调用失败！");
            }
            // 5. 向客户端进行响应
            response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
        }           
        void PutMultiFile      (const httplib::Request& request,httplib::Response &response)
        {
            PutMultiFileReq req;
            PutMultiFileRsp rsp;
            auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            };
            bool ret = req.ParseFromString(request.body);
            if (ret == false) 
            {
                LOG_ERROR("上传多个文件请求正文反序列化失败！");
                return err_response("上传多个文件请求正文反序列化失败！");
            }
            // 2. 客户端身份识别与鉴权
            std::string ssid = req.session_id();
            auto uid = _redis_session->uid(ssid);
            if (!uid) 
            {
                LOG_ERROR("{} 获取登录会话关联用户信息失败！", ssid);
                return err_response("获取登录会话关联用户信息失败！");
            }
            req.set_user_id(*uid);
            // 3. 将请求转发给文件子服务进行业务处理
            auto channel = _mm_channels->choose(_file_service_name);
            if (!channel) 
            {
                LOG_ERROR("{} 未找到可提供业务处理的文件子服务节点！", req.request_id());
                return err_response("未找到可提供业务处理的文件子服务节点！");
            }
            yu::FileService_Stub stub(channel.get());
            brpc::Controller cntl;
            stub.PutMultiFile(&cntl, &req, &rsp, nullptr);
            if (cntl.Failed()) 
            {
                LOG_ERROR("{} 文件子服务调用失败！", req.request_id());
                return err_response("文件子服务调用失败！");
            }
            // 5. 向客户端进行响应
            response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
        }          
        void SpeechRecognition    (const httplib::Request& request,httplib::Response &response)
        {
            SpeechRecognitionReq req;
            SpeechRecognitionRsp rsp;
            auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void 
            {
                rsp.set_success(false);
                rsp.set_errmsg(errmsg);
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            };
            bool ret = req.ParseFromString(request.body);
            if (ret == false) 
            {
                LOG_ERROR("语音识别请求正文反序列化失败！");
                return err_response("语音识别请求正文反序列化失败！");
            }
            // 2. 客户端身份识别与鉴权
            std::string ssid = req.session_id();
            auto uid = _redis_session->uid(ssid);
            if (!uid) 
            {
                LOG_ERROR("{} 获取登录会话关联用户信息失败！", ssid);
                return err_response("获取登录会话关联用户信息失败！");
            }
            req.set_user_id(*uid);
            // 3. 将请求转发给语音识别子服务进行业务处理
            auto channel = _mm_channels->choose(_speech_service_name);
            if (!channel) 
            {
                LOG_ERROR("{} 未找到可提供业务处理的语音识别子服务节点！", req.request_id());
                return err_response("未找到可提供业务处理的语音识别子服务节点！");
            }
            yu::SpeechService_Stub stub(channel.get());
            brpc::Controller cntl;
            stub.SpeechRecognition(&cntl, &req, &rsp, nullptr);
            if (cntl.Failed()) 
            {
                LOG_ERROR("{} 语音识别子服务调用失败！", req.request_id());
                return err_response("语音识别子服务调用失败！");
            }
            // 5. 向客户端进行响应
            response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
        }       
    private:
        session::ptr _redis_session;
        status::ptr _redis_status;
        std::string _user_service_name;
        std::string _file_service_name;
        std::string _speech_service_name;
        std::string _message_service_name;
        std::string _transmite_service_name;
        std::string _friend_service_name;
        ServiceManager::ptr _mm_channels;
        Discovery::ptr _service_discoverer;
        Connection::ptr _connections;
        server_t _ws_server;
        httplib::Server _http_server;
        std::thread _http_thread;
    };
    class GatewayServerBuilder
    {
    public:
        //构造redis客户端对象
            void make_redis_object(const std::string &host,
                int port,
                int db,
                bool keep_alive) 
            {
                _redis_client = RedisFactory::create(host, port, db, keep_alive);
            }
            //用于构造服务发现客户端&信道管理对象
            void make_discovery_object(const std::string &reg_host,
                const std::string &base_service_name,
                const std::string &file_service_name,
                const std::string &speech_service_name,
                const std::string &message_service_name,
                const std::string &friend_service_name,
                const std::string &user_service_name,
                const std::string &transmite_service_name) 
                {
                _file_service_name      = file_service_name;
                _speech_service_name    = speech_service_name;
                _message_service_name   = message_service_name;
                _friend_service_name    = friend_service_name;
                _user_service_name      = user_service_name;
                _transmite_service_name = transmite_service_name;
                _mm_channels = std::make_shared<ServiceManager>();
                _mm_channels->declared(file_service_name);
                _mm_channels->declared(speech_service_name);
                _mm_channels->declared(message_service_name);
                _mm_channels->declared(friend_service_name);
                _mm_channels->declared(user_service_name);
                _mm_channels->declared(transmite_service_name);
                auto put_cb = std::bind(&ServiceManager::onServiceOnline, _mm_channels.get(), std::placeholders::_1, std::placeholders::_2);
                auto del_cb = std::bind(&ServiceManager::onServiceOffline, _mm_channels.get(), std::placeholders::_1, std::placeholders::_2);
                _service_discoverer = std::make_shared<Discovery>(reg_host, base_service_name, put_cb, del_cb);
            }
            void make_server_object(int websocket_port, int http_port) 
            {
                _websocket_port = websocket_port;
                _http_port = http_port;
            }
            //构造RPC服务器对象
            GatewayServer::ptr build() 
            {
                if (!_redis_client) 
                {
                    LOG_ERROR("还未初始化Redis客户端模块！");
                    abort();
                }
                if (!_service_discoverer) 
                {
                    LOG_ERROR("还未初始化服务发现模块！");
                    abort();
                }
                if (!_mm_channels) 
                {
                    LOG_ERROR("还未初始化信道管理模块！");
                    abort();
                }
                GatewayServer::ptr server = std::make_shared<GatewayServer>(
                    _websocket_port, _http_port, _redis_client, _mm_channels, 
                    _service_discoverer, _user_service_name, _file_service_name,
                    _speech_service_name, _message_service_name, 
                    _transmite_service_name, _friend_service_name);
                return server;
            }
        private:
            int _websocket_port;
            int _http_port;

            std::shared_ptr<sw::redis::Redis> _redis_client;

            std::string _file_service_name;
            std::string _speech_service_name;
            std::string _message_service_name;
            std::string _friend_service_name;
            std::string _user_service_name;
            std::string _transmite_service_name;
            ServiceManager::ptr _mm_channels;
            Discovery::ptr _service_discoverer;
    };
}