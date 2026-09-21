#pragma once
#include "mysql.hpp"
#include "chat_session.hxx"
#include "chat_session-odb.hxx"
#include "mysql_session_member.hpp"
#include <vector>
namespace yu
{
    class Chatsessiontable
    {
        public:
            using ptr = std::shared_ptr<Chatsessiontable>;
            Chatsessiontable(const std::shared_ptr<odb::core::database>& db)
                : _db(db) {}
            bool insert(Chat_session& cs)
            {
                 try {
                    // 获取事务对象开启事务
                    odb::transaction trans(_db->begin());
                    _db->persist(cs);
                    // 5. 提交事务
                    trans.commit();
                } catch (std::exception &e) {
                    LOG_ERROR("新增会话出错:{}-{}-{}", cs.chat_session_id(), cs.chat_session_name(), e.what());
                    return false;
                }
                return true;
            }
            bool remove(const std::string& chat_session_id)
            {
                try {
                    // 获取事务对象开启事务
                    odb::transaction trans(_db->begin());
                    typedef odb::query<Chat_session> query;
                    _db->erase_query<Chat_session>(query::chat_session_id == chat_session_id);
                    typedef odb::query<ChatSessionMember> mquery;
                    _db->erase_query<ChatSessionMember>(mquery::session_id == chat_session_id);
                    // 5. 提交事务
                    trans.commit();
                } catch (std::exception &e) {
                    LOG_ERROR("删除会话出错:{}-{}", chat_session_id, e.what());
                    return false;
                }
                return true;
            }
            bool remove(const std::string& uid, const std::string& pid)
            {
                //单聊的删除
                try
                {  
                    odb::transaction trans(_db->begin());
                    std::string q = "csm1.user_id = '" + uid + "' and csm2.user_id = '" + pid +
                                    "' and css._chat_session_type = 1";
                    auto ret = _db->query_one<yu::SingleChatSession>(q);
                    if (!ret)
                    {
                        trans.commit();
                        return true;
                    }
                    std::string mssid = ret->chat_session_id;
                    typedef odb::query<Chat_session> mquery;
                    _db->erase_query<Chat_session>(mquery::chat_session_id == mssid);
                    typedef odb::query<ChatSessionMember> cquery;
                    _db->erase_query<ChatSessionMember>(cquery::session_id == mssid);
                    trans.commit();
                }
                catch(std::exception& e)
                {
                    LOG_ERROR("删除会话出错:{}-{}-{}", uid, pid, e.what());
                    return false;
                }
                return true;
            }
            std::shared_ptr<Chat_session> select(const std::string& ssid)
            {
                std::shared_ptr<Chat_session> res;
                try
                {
                    odb::transaction trans(_db->begin());
                    typedef odb::query<Chat_session> query;
                    res.reset(_db->query_one<Chat_session>(query::chat_session_id == ssid));
                    trans.commit();
                }
                catch(std::exception& e)
                {
                    LOG_ERROR("查询会话出错:{}", e.what());
                }
                return res;
            }
            std::vector<yu::SingleChatSession> SingleChatSession(const std::string& uid)
            {
                std::vector<yu::SingleChatSession> res;
                try
                {
                    odb::transaction trans(_db->begin());
                    std::string q = "csm1.user_id = '" + uid + "' and csm2.user_id <> '" + uid +
                                    "' and css._chat_session_type = 1";
                    typedef odb::result<yu::SingleChatSession> result;
                    result r(_db->query<yu::SingleChatSession>(q));
                    for(result::iterator i = r.begin(); i != r.end(); ++i)
                    {
                        res.push_back(*i);
                    }
                    trans.commit();
                }
                catch(std::exception& e)
                {
                    LOG_ERROR("查询单聊会话出错:{}", e.what());
                }
                return res;
            }
            std::vector<yu::GroupChatSession> GroupChatSession(const std::string& uid)
            {
                std::vector<yu::GroupChatSession> res;
                try
                {
                    odb::transaction trans(_db->begin());
                    std::string q = "csm1.user_id = '" + uid + "' and css._chat_session_type = 2";
                    typedef odb::result<yu::GroupChatSession> result;
                    result r(_db->query<yu::GroupChatSession>(q));
                    for(result::iterator i = r.begin(); i != r.end(); ++i)
                    {
                        res.push_back(*i);
                    }
                    trans.commit();
                }
                catch(std::exception& e)
                {
                    LOG_ERROR("查询单聊会话出错:{}", e.what());
                }
                return res;
            }
        private:
            std::shared_ptr<odb::core::database> _db;
    };
}
