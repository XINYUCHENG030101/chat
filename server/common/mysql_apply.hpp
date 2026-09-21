#pragma once
#include "mysql.hpp"
#include "friend_apply.hxx"
#include "friend_apply-odb.hxx"
namespace yu
{
    class Applytable
    {
        public:
            using ptr = std::shared_ptr<Applytable>;
            Applytable(const std::shared_ptr<odb::core::database> &db)
            :_db(db){}
            // 新增申请
            bool insert(Friend_apply &apply)
            {
                try {
                    // 获取事务对象开启事务
                    odb::transaction trans(_db->begin());
                    _db->persist(apply);
                    // 5. 提交事务
                    trans.commit();
                } catch (std::exception &e) {
                    LOG_ERROR("新增申请出错:{}-{}-{}", apply.user_id(), apply.peer_id(), e.what());
                    return false;
                }
                return true;
            }
            bool exist(const std::string& uid,const std::string& pid)
            {
                typedef odb::query<Friend_apply> query;
                typedef odb::result<Friend_apply> result;
                result r;
                bool flag = false;
                try 
                {
                    odb::transaction trans(_db->begin());
                    r = _db->query<Friend_apply>(query::user_id == uid && query::peer_id == pid);
                    flag = !r.empty();
                    trans.commit();
                } 
                catch (std::exception &e) 
                {
                        LOG_ERROR("判断申请是否存在失败:{}-{}-{}",uid,pid,e.what());
                }
                return flag;
            }
            bool remove(const std::string& uid,const std::string& pid)
            {
                try
                {
                    // 获取事务对象开启事务
                    odb::transaction trans(_db->begin());
                    typedef odb::query<Friend_apply> query;
                    typedef odb::result<Friend_apply> result;
                    _db->erase_query<Friend_apply>(query::user_id == uid && query::peer_id == pid);
                    // 5. 提交事务
                    trans.commit();
                }
                catch(std::exception& e)
                {
                    LOG_ERROR("删除申请出错:{}-{}-{}", uid, pid, e.what());
                    return false;
                }
                return true;
            }
            std::vector<std::string> applyuser(const std::string& uid)
            {
                std::vector<std::string> res;
                try
                {
                    // 获取事务对象开启事务
                    odb::transaction trans(_db->begin());
                    typedef odb::query<Friend_apply> query;
                    typedef odb::result<Friend_apply> result;
                    result r(_db->query<Friend_apply>(query::peer_id == uid));
                    for (auto it = r.begin(); it != r.end(); ++it) {
                        res.push_back(it->user_id());
                    }
                    trans.commit();
                }
                catch(std::exception& e)
                {
                    LOG_ERROR("获取申请用户出错:{}-{}", uid, e.what());
                }
                return res;
            }
            bool exist(const std::string& eid)
            {
                typedef odb::query<Friend_apply> query;
                typedef odb::result<Friend_apply> result;
                try
                {
                    odb::transaction trans(_db->begin());
                    result r(_db->query<Friend_apply>(query::event_id == eid));
                    bool flag = !r.empty();
                    trans.commit();
                    return flag;
                }
                catch(std::exception& e)
                {
                    LOG_ERROR("判断申请事件是否存在失败:{}-{}", eid, e.what());
                    return false;
                }
            }
        private:
            std::shared_ptr<odb::core::database> _db;
    };
}
