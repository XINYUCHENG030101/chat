#pragma once
#include "mysql.hpp"
#include "ChatSessionMember.hxx"
#include "ChatSessionMember-odb.hxx"
namespace yu
{
    class ChatSessionMemberTable
    {
        public:
            using ptr = std::shared_ptr<ChatSessionMemberTable>;
            ChatSessionMemberTable(const std::shared_ptr<odb::core::database>& db)
            :_db(db)
            {}
        //向指定会话中添加单个成员
        bool append(const std::shared_ptr<ChatSessionMember>& csm)
        {
             try 
                {
                    //获取事务对象开启事务
                    odb::transaction trans(_db->begin());
                    _db->persist(*csm);
                    //5. 提交事务
                    trans.commit();
                }
                catch (std::exception &e) 
                {
                    LOG_ERROR("添加单个成员失败:{}{}",csm->user_id(),e.what());
                    return false;
                }
                return true;
        }
        //向指定会话中添加多个成员
        bool append(std::vector<ChatSessionMember>& csm_lists)
        {
            try
            {
                odb::transaction trans(_db->begin());
                ChatSessionMember csm;
                for(int i = 0;i<csm_lists.size();i++)
                {
                    _db->persist(csm_lists[i]);
                }
                //5. 提交事务
                trans.commit();
            }
            catch(std::exception &e)
            {
                LOG_ERROR("添加多个成员失败:{}-{}-{}",csm_lists[0].session_id(),csm_lists.size(),e.what());
                return false;
            }
            return true;
        }
        //从指定会话中删除单个成员
        bool remove(ChatSessionMember& csm)
        {
            try
            {
                odb::transaction trans(_db->begin());
                typedef odb::query<ChatSessionMember> query;
                typedef odb::result<ChatSessionMember> result;
                _db->erase_query<ChatSessionMember>(query::session_id == csm.session_id()
                &&query::user_id == csm.user_id());
                //5. 提交事务
                trans.commit();

            }
            catch(std::exception& e)
            {
                LOG_ERROR("删除单个成员失败{}-{}-{}",csm.session_id(),csm.user_id(),e.what());
                return false;
            }
            return true;
        }
        //通过会话ID，获取会话的所有成员ID
        std::vector<std::string> get_user_id(std::string& ssid)
        {
            std::vector<std::string> res;
            try
            {
                odb::transaction trans(_db->begin());
                typedef odb::query<ChatSessionMember> query;
                typedef odb::result<ChatSessionMember> result;
                result r(_db->query<ChatSessionMember>(query::session_id == ssid));
                for(auto it = r.begin();it!=r.end();it++)
                {
                    res.push_back(it->user_id());
                }
                trans.commit();
            }
            catch(std::exception& e)
            {
                LOG_ERROR("获取会话用户失败{}-{}",ssid,e.what());
                return std::vector<std::string>();
            }
            return res;
        }
        //删除会话所有成员：在删除会话的时候使用
        bool remove(std::vector<ChatSessionMember>& csm_lists)
        {
            try
            {
                odb::transaction trans(_db->begin());
                typedef odb::query<ChatSessionMember> query;
                typedef odb::result<ChatSessionMember> result;
                for(auto& csm : csm_lists)
                {
                    _db->erase_query<ChatSessionMember>(query::session_id == csm.session_id());
                }
                trans.commit();
            }
            catch(std::exception& e)
            {
                LOG_ERROR("批量删除失败{}-{}",csm_lists[0].session_id(),e.what());
                return false;
            }
            return true;
        }
        private:
            std::shared_ptr<odb::core::database> _db;
    }; 
}