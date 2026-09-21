#pragma once
#include<memory>
#include"user.hxx"
#include"user-odb.hxx"
#include <jsoncpp/json/json.h>
#include"logger.hpp" //日志
#include"usr.pb.h"
#include"file.pb.h"
#include"base.pb.h"
#include"etcd.hpp" //服务注册
#include <brpc/server.h>
#include <butil/logging.h>
#include <iostream>
#include"mysql.hpp"
#include"data_es.hpp"
#include"data_redis.hpp"
#include"mysql_user.hpp"
#include"channel.hpp"
#include"dms.hpp"
#include"utils.hpp"

namespace yu
{
    class UserServiceIpml:public yu::UserService
    {
    public:
        UserServiceIpml(
            const std::shared_ptr<elasticlient::Client> &es_client,
            const std::shared_ptr<sw::redis::Redis> &redis_client,
            const std::shared_ptr<odb::core::database> &db,
            const ServiceManager::ptr &channel_manager,
            const std::string &file_service_name,
            const EmailVerificationSender::ptr &sender
        )
        :_es_user(std::make_shared<ESuser>(es_client)),  
        _redis_session(std::make_shared<session>(redis_client)),
        _redis_status(std::make_shared<status>(redis_client)),
        _redis_code(std::make_shared<code>(redis_client)),
        _mysql_user(std::make_shared<Usertable>(db)),
        _file_service_name(file_service_name),
        _mm_channels(channel_manager),
        _dms_sender(sender)
        {}
        ~UserServiceIpml(){}
        bool nickname_check(const std::string& nickname)
        {
            return nickname.size()<22;
        }
        bool password_check(const std::string &password)
        {
            //校验长度
            if(password.size()>15 || password.size()< 3 )
                return false;
            //校验密码内容
            for(int i = 0;i<password.size();i++)
            {
                if(
                    !((password[i]>'a' && password[i]<'z')
                        || (password[i]>'A'&&password[i]<'Z')
                        ||(password[i]>0&&password[i]<9)
                        ||(password[i] == '-' || password[i] == '_')
                    )
                )
                {
                    return false;
                }
            }
            return true;
        }
        virtual void UserRegister(::google::protobuf::RpcController* controller,
                       const ::yu::UserRegisterReq* request,
                       ::yu::UserRegisterRsp* response,
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
            //1. 从请求中取出昵称和密码
            std::string nickname = request->nickname();
            std::string password = request->password();
            //2. 检查昵称是否合法（只能包含字母，数字，连字符-，下划线_，长度限制3~15之间）
            bool ret = nickname_check(nickname);
            if(ret == false)
            {
                LOG_ERROR("昵称长度不合法！{}-{}",request->request_id(),nickname.size());
                err_ret(request->request_id(),"昵称长度不合法");
                return;
            }
            //3. 检查密码是否合法（只能包含字母，数字，长度限制6~15之间）
            ret = password_check(password);
            if(ret == false)
            {
                LOG_ERROR("密码长度不合法！{}-{}",request->request_id(),password.size());
                err_ret(request->request_id(),"密码长度不合法");
                return;
            }
            //4. 根据昵称在数据库进行判断是否昵称已存在
            auto user = _mysql_user->select_by_nickname(nickname);
            if(user)
            {
                LOG_ERROR("昵称已经存在{}",nickname);
                err_ret(request->request_id(),"昵称已经存在");
                return;
            }
            //5. 向数据库新增数据
            std::string uid = uuid();
            user = std::make_shared<yu::user>(uid,nickname,password);
            ret = _mysql_user->insert(user);
            if(ret == false)
            {
                LOG_ERROR("向数据库中新增数据失败{}",request->request_id());
                err_ret(request->request_id(),"向数据库中新增数据失败");
                return;
            }
            //6. 向ES服务器中新增用户信息
            ret = _es_user->appenddata(uid,nickname,"","","");
            if(ret == false)
            {
                LOG_ERROR("向ES搜索引擎中添加数据失败{}",request->request_id());
                err_ret(request->request_id(),"向ES搜索引擎中添加数据失败");
                return;
            }
            //7. 组织响应，进行成功与否的响应即可。
            response->set_request_id(request->request_id());
            response->set_success(true);

        }
        virtual void UserLogin(::google::protobuf::RpcController* controller,
                            const ::yu::UserLoginReq* request,
                            ::yu::UserLoginRsp* response,
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
            //1. 从请求中取出昵称和密码
            std::string nickname = request->nickname();
            std::string password = request->password();
            //2. 通过昵称获取用户信息，进行密码是否一致的判断
            auto user1 = _mysql_user->select_by_nickname(nickname);
            if(!user1 || user1->password()!=password)
            {
                LOG_ERROR("密码不一致{}",request->request_id());
                err_ret(request->request_id(),"密码不一致");
                return;
            }
            //3. 根据redis 中的登录标记信息是否存在判断用户是否已经登录。
            auto ret = _redis_status->exist(user1->user_id());
            if(ret == false)
            {
                LOG_ERROR("用户已在别的设备登录{}",request->request_id());
                err_ret(request->request_id(),"用户已在别的设备登录");
                return;
            }
            //4. 构造会话ID，生成会话键值对，向redis 中添加会话信息以及登录标记信息
            std::string ssid = uuid();
            _redis_session->append(ssid,user1->user_id());
            //5. 组织响应，返回生成的会话ID  
            response->set_request_id(request->request_id());
            response->set_success(true);
            response->set_login_session_id(ssid); 
        }
        bool phone_check(const std::string &phone)
        {
            // 例如 nickname@qq.com
            //校验长度
            if(phone.size()< 12||phone.size()>20)
                return false;
            size_t at_pos = phone.find('@');
            if (at_pos == std::string::npos || at_pos == 0) 
                return false;   // 没有 @ 或 @ 在开头
            //提取 QQ 号部分并检查是否为纯数字
            std::string num_part = phone.substr(0,at_pos);
            if(num_part.size()==0 )
                return false;
            if(num_part[0] == 0)
                return false;
            for(auto e : num_part)
            {
                if(!std::isdigit(e))
                {
                    return false;
                }
            }
            //检查域名部分是否为 "qq.com"
             std::string domain_part = phone.substr(at_pos + 1);
            if (domain_part != "qq.com") 
            {
                return false;   // 可根据需要扩展支持 vip.qq.com 等
            }
            return true;
        }
        virtual void GetPhoneVerifyCode(::google::protobuf::RpcController* controller,
                            const ::yu::PhoneVerifyCodeReq* request,
                            ::yu::PhoneVerifyCodeRsp* response,
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
            //1. 从请求中取出手机号码
            std::string phone = request->phone_number();
            //2. 验证手机号码格式是否正确（必须以1开始，第二位3~9之间，后边9个数字字符）
            //在这里将手机号改成了qq号
            auto ret = phone_check(phone);
            if(ret == false)
            {
                LOG_ERROR("格式不正确{}",phone);
                err_ret(request->request_id(),"格式不正确");
                return;
            }
            //3. 生成4位随机验证码
            std::string code = vcode();
            std::string code_id = uuid();
            //4. 基于短信平台SDK发送验证码(由于云平台没办法发送这里换成了邮件)
            ret = _dms_sender->sendVerificationCode(phone,code);
            if(ret == false)
            {
                LOG_ERROR("{},验证码发送失败{}",phone,code_id);
                err_ret(request->request_id(),"验证码发送失败");
                return;
            }

            //5. 构造验证码ID，添加到redis验证码映射键值索引中
            _redis_code->append(code,code_id);
            //6. 组织响应，返回生成的验证码ID
            response->set_request_id(request->request_id());
            response->set_success(true);
            response->set_verify_code_id(code_id);
        }
        virtual void PhoneRegister(::google::protobuf::RpcController* controller,
                            const ::yu::PhoneRegisterReq* request,
                            ::yu::PhoneRegisterRsp* response,
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
            //1. 从请求中取出手机号码和验证码
            std::string phone = request->phone_number();
            std::string verify_code = request->verify_code(); 
            std::string verify_code_id = request->verify_code_id();
            //2. 检查注册手机号码是否合法 
            auto ret = phone_check(phone);
            if(ret == false)
            {

                LOG_ERROR("手机号码不合法{},{}",phone,request->request_id());
                err_ret(request->request_id(),"号码不合法");
                return;
            }
            //3. 从redis数据库中进行验证码ID-验证码一致性匹配
            auto vcode = _redis_code->get_code(verify_code_id);
            if(vcode != verify_code)
            {
                LOG_ERROR("{}redis中的验证码与请求中的不一致{}-{}",request->request_id(),verify_code_id,verify_code);
                err_ret(request->request_id(),"redis中的验证码与请求中的不一致");
                return;
            }
            //4. 通过数据库查询判断手机号是否已经注册过 
            auto user = _mysql_user->select_by_phone(phone);
            if(user)
            {
                LOG_ERROR("{}该账号已被注册{}",request->request_id(),phone);
                err_ret(request->request_id(),"该账号已被注册");
                return;
            }
            //5. 向数据库新增用户信息 
            std::string uid = uuid();
            user = std::make_shared<yu::user>(uid,phone);
            ret = _mysql_user->insert(user);
            if(ret == false)
            {
                LOG_ERROR("向数据库中新增用户失败{}-{}",request->request_id(),phone);
                err_ret("向数据库中新增用户失败",request->request_id());
                return;
            }
            //6. 向ES服务器中新增用户信息 
            ret = _es_user->appenddata(uid,uid,phone,"","");
            if(ret == false)
            {
                LOG_ERROR("向ES中新增用户失败{}-{}",request->request_id(),phone);
                err_ret("向ES中新增用户失败",request->request_id());
                return;
            }
            //7.组织响应，返回注册成功与否
            response->set_request_id(request->request_id());
            response->set_success(true);
        }
        virtual void PhoneLogin(::google::protobuf::RpcController* controller,
                            const ::yu::PhoneLoginReq* request,
                            ::yu::PhoneLoginRsp* response,
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
            //1. 从请求中取出手机号码和验证码ID，以及验证码。
            std::string phone = request->phone_number();
            std::string verify_code = request->verify_code(); 
            std::string verify_code_id = request->verify_code_id();
            //2. 检查注册手机号码是否合法
            auto ret = phone_check(phone);
            if(ret == false)
            {
                LOG_ERROR("手机号码不合法{},{}",phone,request->request_id());
                err_ret(request->request_id(),"号码不合法");
                return;
            }
            //3. 根据手机号从数据库进行用户信息查询，判断用用户是否存在
            auto user = _mysql_user->select_by_phone(phone);
            if(!user)
            {
                LOG_ERROR("用户没有注册{}-{}",request->request_id(),phone);
                err_ret(request->request_id(),"用户没有注册");
                return;
            }
            //4. 从redis数据库中进行验证码ID-验证码一致性匹配
            auto vcode = _redis_code->get_code(verify_code_id);
            if(vcode != verify_code)
            {
                LOG_ERROR("{}redis中的验证码与请求中的不一致{}-{}",request->request_id(),verify_code_id,verify_code);
                err_ret(request->request_id(),"redis中的验证码与请求中的不一致");
                return;
            }
            //5. 根据redis中的登录标记信息是否存在判断用户是否已经登录。
            ret = _redis_status->exist(user->user_id());
            if(ret == true)
            {
                LOG_ERROR("用户已登录{}-{}",request->request_id(),phone);
                err_ret(request->request_id(),"用户已登录");
                return;
            }
            //6. 构造会话ID，生成会话键值对，向redis中添加会话信息以及登录标记信息
            std::string ssid = uuid();
            _redis_session->append(ssid,user->user_id());
            _redis_status->append(user->user_id());
            //7. 组织响应，返回生成的会话ID
            response->set_request_id(request->request_id());
            response->set_success(true);
            response->set_login_session_id(ssid);
        }
//以下都是用户登录后的操作
        virtual void GetUserInfo(::google::protobuf::RpcController* controller,
                            const ::yu::GetUserInfoReq* request,
                            ::yu::GetUserInfoRsp* response,
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
            //1. 从请求中取出用户ID
            std::string uid = request->user_id();
            //2. 通过用户ID，从数据库中查询用户信息
            auto user = _mysql_user->select_by_id(uid);
            if(!user)
            {
                LOG_ERROR("未查询到用户信息{}-{}",request->request_id(),uid);
                err_ret(request->request_id(),"未查询到用户信息");
                return;
            }
            //3. 根据用户信息中的头像ID，从文件服务器获取头像文件数据，组织完整用户信息
            UserInfo* user_info = response->mutable_user_info();
            user_info->set_user_id(user->user_id());
            user_info->set_nickname(user->nickname());
            user_info->set_phone(user->phone());
            user_info->set_description(user->description());
            if(!user->avatar_id().empty())
            {
                auto channel = _mm_channels->choose(_file_service_name);
                if(!channel)
                {
                    LOG_ERROR("未找到文件子服务节点{}-{}-{}",request->request_id(),_file_service_name,uid);
                    err_ret(request->request_id(),"未找到文件子服务节点");
                    return;
                }
                yu::FileService_Stub stub(channel.get());
                yu::GetSingleFileReq req;
                yu::GetSingleFileRsp rsp;
                req.set_request_id(request->request_id());
                req.set_file_id(user->avatar_id());
                brpc::Controller cntl;
                stub.GetSingleFile(&cntl,&req,&rsp,nullptr);
                if(cntl.Failed() == true||rsp.success() == false)
                {
                    LOG_ERROR("文件子服务调用失败{}-{}",request->request_id(),cntl.ErrorText());
                    err_ret(request->request_id(),"文件子服务调用失败");
                    return;
                }
                user_info->set_avatar(rsp.file_data().file_content());
            }
            //4. 组织响应，返回用户信息
            response->set_request_id(request->request_id());
            response->set_success(true);
        }
        virtual void GetMultiUserInfo(::google::protobuf::RpcController* controller,
                            const ::yu::GetMultiUserInfoReq* request,
                            ::yu::GetMultiUserInfoRsp* response,
                            ::google::protobuf::Closure* done)
        {
            //批量获取用户信息
            brpc::ClosureGuard rpc_guard(done);
            auto err_ret = [this,response](const std::string &rid,
                                            const std::string &errmsg)
            {
                response->set_request_id(rid);
                response->set_success(false);
                response->set_errmsg(errmsg);
                return;
            };
            std::vector<std::string> uid_lists;
            for(int i = 0;i<request->users_id_size();i++)
            {
                uid_lists.push_back(request->users_id(i));
            }
            auto users = _mysql_user->select_multi_users(uid_lists);
            if(users.size()!=request->users_id_size())
            {
                LOG_ERROR("与数据库中所查找到的用户信息不一致{}-{}-{}"
                    ,request->request_id(),request->users_id_size(),users.size());
                err_ret(request->request_id(),"与数据库中所查找到的用户信息不一致");
                return;
            }
            auto channel = _mm_channels->choose(_file_service_name);
            if(!channel)
            {
                LOG_ERROR("未找到文件子服务节点{}-{}",request->request_id(),_file_service_name);
                err_ret(request->request_id(),"未找到文件子服务节点");
                return;
            }
            yu::FileService_Stub stub(channel.get());
            yu::GetMultiFileReq req;
            yu::GetMultiFileRsp rsp;
            req.set_request_id(request->request_id());
            for(auto &user:users)
            {
                if(user.avatar_id().empty())
                    continue;
                req.add_file_id_list(user.avatar_id());
            }
            brpc::Controller cntl;
            stub.GetMultiFile(&cntl,&req,&rsp,nullptr);
            if(cntl.Failed() == true||rsp.success() == false)
            {
                LOG_ERROR("文件子服务调用失败{}-{}",request->request_id(),cntl.ErrorText());
                err_ret(request->request_id(),"文件子服务调用失败");
                return;
            }
            for(auto user:users)
            {
                auto user_map = response->mutable_users_info();//本次请求要响应的用户信息map
                auto file_map = rsp.mutable_file_data(); //这是批量文件请求响应中的map 
                UserInfo user_info;
                user_info.set_user_id(user.user_id());
                user_info.set_nickname(user.nickname());
                user_info.set_description(user.description());
                user_info.set_phone(user.phone());
                user_info.set_avatar((*file_map)[user.avatar_id()].file_content());
                (*user_map)[user_info.user_id()] = user_info;
            }
                response->set_request_id(request->request_id());
                response->set_success(true);

        }
        virtual void SetUserAvatar(::google::protobuf::RpcController* controller,
                            const ::yu::SetUserAvatarReq* request,
                            ::yu::SetUserAvatarRsp* response,
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
            //1.从请求中取出用户ID与头像数据
            std::string uid = request->user_id();
            //2. 从数据库通过用户ID进行用户信息查询，判断用户是否存在
            auto user = _mysql_user->select_by_id(uid);
            if(!user)
            {
                LOG_ERROR("未查询到用户信息{}-{}",request->request_id(),uid);
                err_ret(request->request_id(),"未查询到用户信息");
                return;
            }
            //3. 上传头像文件到文件子服务，
            auto channel = _mm_channels->choose(_file_service_name);
            if(!channel)
            {
                LOG_ERROR("未找到文件子服务节点{}-{}",request->request_id(),_file_service_name);
                err_ret(request->request_id(),"未找到文件子服务节点");
                return;
            }
            yu::FileService_Stub stub(channel.get());
            yu::PutSingleFileReq req;
            yu::PutSingleFileRsp rsp;
            req.set_request_id(request->request_id());
            req.mutable_file_data()->set_file_name("");
            req.mutable_file_data()->set_file_size(request->avatar().size());
            req.mutable_file_data()->set_file_content(request->avatar());
            brpc::Controller cntl;
            stub.PutSingleFile(&cntl,&req,&rsp,nullptr);
            if (cntl.Failed() == true || rsp.success() == false) 
            {
                LOG_ERROR("{} - 文件子服务调用失败：{}！", request->request_id(), cntl.ErrorText());
                return err_ret(request->request_id(), "文件子服务调用失败!");
            }
            std::string avatar_id = rsp.file_info().file_id();
            //4. 将返回的头像文件ID更新到数据库中
            user->avatar_id(avatar_id);
            bool ret = _mysql_user->update(user);
            if(ret == false)
            {
                LOG_ERROR("数据库头像更新失败{}-{}",request->request_id(),avatar_id);
                err_ret(request->request_id(),"数据库头像更新失败");
                return;
            }
            //5. 更新ES服务器中用户信息
            ret = _es_user->appenddata(user->user_id(),user->nickname(),
                user->phone(),user->description(),user->avatar_id());
            if(ret == false)
            {
                LOG_ERROR("更新搜索引擎用户头像ID失败{}-{}",request->request_id(),avatar_id);
                err_ret(request->request_id(),"更新搜索引擎用户头像ID失败");
                return;
            }
            //6. 组织响应，返回更新成功与否
            response->set_request_id(request->request_id());
            response->set_success(true);
        }
        virtual void SetUserNickname(::google::protobuf::RpcController* controller,
                            const ::yu::SetUserNicknameReq* request,
                            ::yu::SetUserNicknameRsp* response,
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
            //1. 从请求中取出用户ID与新的昵称
            std::string uid = request->user_id();
            std::string nickname = request->nickname();
            //2. 判断昵称格式是否正确
            bool ret = nickname_check(nickname);
            if(ret == false)
            {
                LOG_ERROR("昵称不合法{}-{}",request->request_id(),nickname);
                err_ret(request->request_id(),"昵称不合法");
            }
            //3. 从数据库通过用户ID进行用户信息查询，判断用户是否存在
            auto user = _mysql_user->select_by_id(uid);
            if(!user)
            {
                LOG_ERROR("未找到用户信息{}-{}",request->request_id(),uid);
                err_ret(request->request_id(),"未找到用户信息");
                return;
            }
            //4. 将新的昵称更新到数据库中
            user->nickname(nickname);
            ret = _mysql_user->update(user);
            if(ret == false)
            {
                LOG_ERROR("更新数据库昵称失败{}-{}",request->request_id(),nickname);
                err_ret(request->request_id(),"更新数据库昵称失败");
                return;
            }
            //5. 更新ES服务器中用户信息
            ret = _es_user->appenddata(user->user_id(),user->nickname(),
                user->phone(),user->description(),user->avatar_id());
            if(ret == false)
            {
                LOG_ERROR("更新搜索引擎用户昵称ID失败{}-{}",request->request_id(),nickname);
                err_ret(request->request_id(),"更新搜索引擎用户昵称失败");
                return;
            }
            //6. 组织响应，返回更新成功与否
            response->set_request_id(request->request_id());
            response->set_success(true);
        }
        virtual void SetUserDescription(::google::protobuf::RpcController* controller,
                            const ::yu::SetUserDescriptionReq* request,
                            ::yu::SetUserDescriptionRsp* response,
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
            //1. 从请求中取出用户ID与新的签名
            std::string uid = request->user_id();
            std::string description = request->description();
            //2. 从数据库通过用户ID进行用户信息查询，判断用户是否存在
            auto user = _mysql_user->select_by_id(uid);
            if(!user)
            {
                LOG_ERROR("未查询到该用户{}-{}",request->request_id(),uid);
                err_ret(request->request_id(),"未查询到该用户");
                return;
            }
            //3. 将新的签名更新到数据库中
            user->description(description);
            bool ret = _mysql_user->update(user);
            if(ret == false)
            {
                LOG_ERROR("更新数据库签名失败{}",request->request_id());
                err_ret(request->request_id(),"更新数据库签名失败");
                return;
            }
            //4. 更新ES服务器中用户信息
             ret = _es_user->appenddata(user->user_id(),user->nickname(),
                user->phone(),user->description(),user->avatar_id());
            if(ret == false)
            {
                LOG_ERROR("更新搜索引擎用户签名失败",request->request_id());
                err_ret(request->request_id(),"更新搜索引擎用户签名失败");
                return;
            }
            //5. 组织响应，返回更新成功与否
            response->set_request_id(request->request_id());
            response->set_success(true);
        }
        virtual void SetUserPhoneNumber(::google::protobuf::RpcController* controller,
                            const ::yu::SetUserPhoneNumberReq* request,
                            ::yu::SetUserPhoneNumberRsp* response,
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
            //1. 从请求中取出手机号码和验证码ID，以及验证码。
            std::string phone = request->phone_number();
            std::string code = request->phone_verify_code();
            std::string code_id = request->phone_verify_code_id();
            //2. 检查注册手机号码是否合法
            bool ret = phone_check(phone);
            if(ret == false)
            {
                LOG_ERROR("手机号码不合法{}-{}",request->request_id(),phone);
                err_ret(request->request_id(),"手机号码不合法");
                return;
            }
            //3. 从redis数据库中进行验证码ID-验证码一致性匹配
            auto vcode = _redis_code->get_code(code_id);
            if(vcode != code)
            {
                LOG_ERROR("{}redis中的验证码与请求中的不一致{}-{}",request->request_id(),code_id,code);
                err_ret(request->request_id(),"redis中的验证码与请求中的不一致");
                return;
            }
            //4. 根据手机号从数据数据进行用户信息查询，判断用用户是否存在
            auto user = _mysql_user->select_by_phone(phone);
            if(!user)
            {
                LOG_ERROR("用户不存在{}-{}",request->request_id(),phone);
                err_ret(request->request_id(),"用户不存在");
                return;
            }
            //5. 将新的手机号更新到数据库中
            user->phone(phone);
            ret = _mysql_user->update(user);
            if(ret == false)
            {
                LOG_ERROR("手机号码数据库更新失败{}-{}",request->request_id(),phone);
                err_ret(request->request_id(),"手机号码数据库更新失败");
                return;
            }
            //6. 更新ES服务器中用户信息
            ret = _es_user->appenddata(user->user_id(),user->nickname(),
                user->phone(),user->description(),user->avatar_id());
            if(ret == false)
            {
                LOG_ERROR("更新搜索引擎用户手机号失败",request->request_id());
                err_ret(request->request_id(),"更新搜索引擎用户手机号失败");
                return;
            }
            //7. 组织响应，返回更新成功与否
            response->set_request_id(request->request_id());
            response->set_success(true);
        }

    private:
        //存储数据的表
        ESuser::ptr _es_user;
        session::ptr _redis_session;
        status::ptr _redis_status;
        code::ptr _redis_code;
        Usertable::ptr _mysql_user;
        //这边是rpc调用客户端相关对象
        std::string _file_service_name;
        ServiceManager::ptr _mm_channels;
        //验证码发送
        EmailVerificationSender::ptr _dms_sender;
    };

    class  UserServer
    {
    public:
        using ptr = std::shared_ptr<UserServer>;
        UserServer( 
            const Discovery::ptr &service_discovery,
            const Registry::ptr &reg_client,
            const std::shared_ptr<elasticlient::Client>& es_client,
            const std::shared_ptr<sw::redis::Redis>& redis_client,
            const std::shared_ptr<odb::core::database>& mysql_client,
            const std::shared_ptr<brpc::Server> &server)
        :_service_discover(service_discovery)
        ,_reg_client(reg_client)
        ,_es_client(es_client)
        ,_redis_client(redis_client)
        ,_mysql_client(mysql_client)
        ,_rpc_server(server)
        {}
        ~UserServer(){}
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
        std::shared_ptr<sw::redis::Redis> _redis_client;
        std::shared_ptr<odb::core::database> _mysql_client;
        std::shared_ptr<brpc::Server> _rpc_server;
    };
    class  UserServerBuilder
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
         //构造redis客户端
        void make_redis_client(const std::string &host,
                const int &port,
                const int &db,
                bool keep_alive )
        {
            _redis_client = RedisFactory::create(host,port,db,keep_alive);
        }
        //验证码发送构造
        void make_sender()
        {
            _dms_sender = std::make_shared<EmailVerificationSender>();
        }
         //构造服务发现客户端以及信道管理对象
        void make_discovery_client(const std::string &host,
                                    const std::string &base_service_name,
                                    const std::string &file_service_name)
        {
            _mm_channels = std::make_shared<ServiceManager>();
            _mm_channels->declared(file_service_name);
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
            if(!_redis_client)
            {
                LOG_ERROR("redis客户端未初始化");
                abort();
            }
            if(!_mysql_client)
            {
                LOG_ERROR("mysql客户端未初始化");
                abort();
            }
            if(!_dms_sender)
            {
                LOG_ERROR("验证码发送服务未初始化");
                abort();
            }
            _rpc_server = std::make_shared<brpc::Server>();
            UserServiceIpml *user_service = new UserServiceIpml(_es_client,_redis_client,_mysql_client,_mm_channels,_file_service_name,_dms_sender);
            int ret = _rpc_server->AddService(user_service, brpc::ServiceOwnership::SERVER_DOESNT_OWN_SERVICE);
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
        UserServer::ptr build()
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
            UserServer::ptr server = std::make_shared<UserServer>(_service_discover,
                _reg_client,_es_client,_redis_client,_mysql_client,_rpc_server);
            return server;
        }

    private:
        Registry::ptr _reg_client;
        std::shared_ptr<elasticlient::Client> _es_client;
        std::shared_ptr<sw::redis::Redis> _redis_client;
        std::shared_ptr<odb::core::database> _mysql_client;
        std::string _file_service_name;
        ServiceManager::ptr _mm_channels;
        Discovery::ptr _service_discover;
        EmailVerificationSender::ptr _dms_sender;
        std::shared_ptr<brpc::Server> _rpc_server;
    };
}