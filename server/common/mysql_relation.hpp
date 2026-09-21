#pragma once
#include "mysql.hpp"
#include "relation.hxx"
#include "relation-odb.hxx"
namespace yu
{
    class Relationtable
    {
        public:
            using ptr = std::shared_ptr<Relationtable>;
            Relationtable(const std::shared_ptr<odb::core::database> &db)
            :_db(db){}
            //接口：新增关系信息
            bool insert(const std::string& uid,const std::string& pid)
            {
                try
                {
                    Relation r1(uid,pid);
                    Relation r2(pid,uid);
                    odb::transaction trans(_db->begin());
                    _db->persist(r1);
                    _db->persist(r2);
                    trans.commit();
                }
                catch (std::exception &e) 
                {
                    LOG_ERROR("新增关系信息出错:{}-{}-{}",uid,pid,e.what());
                    return false;
                }
                return true;
            }
            //移除关系信息
            bool remove(const std::string& uid ,const std::string& pid)
            {
                try 
                {
                    odb::transaction trans(_db->begin());
                    typedef odb::query<Relation> query;
                    typedef odb::result<Relation> result;
                    _db->erase_query<Relation>(query::user_id == uid && query::friend_id == pid);
                    _db->erase_query<Relation>(query::user_id == pid && query::friend_id == uid);
                    // 5. 提交事务
                    trans.commit();
                } 
                catch (std::exception &e) 
                {
                        LOG_ERROR("删除关系信息失败:{}-{}-{}",uid,pid,e.what());
                        return false;
                }
                return true;
            }
            //判断关系是否存在
            bool exist(const std::string& uid,const std::string& pid)
            {
                typedef odb::query<Relation> query;
                typedef odb::result<Relation> result;
                result r;
                bool flag = false;
                try 
                {
                    odb::transaction trans(_db->begin());
                    r = _db->query<Relation>(query::user_id == uid && query::friend_id == pid);
                    flag = !r.empty();
                    trans.commit();
                } 
                catch (std::exception &e) 
                {
                        LOG_ERROR("判断关系是否存在失败:{}-{}-{}",uid,pid,e.what());
                }
                return flag;
            }
            //获取指定用户的好友id
            std::vector<std::string> friends(const std::string& uid)
            {
                std::vector<std::string> res;
                try 
                {
                    odb::transaction trans(_db->begin());
                    typedef odb::query<Relation> query;
                    typedef odb::result<Relation> result;
                    result r = _db->query<Relation>(query::user_id == uid);
                    for (auto it = r.begin(); it != r.end(); ++it) 
                    {
                        res.push_back(it->friend_id());
                    }
                    trans.commit();
                } 
                catch (std::exception &e) 
                {
                    LOG_ERROR("获取指定用户的好友id失败:{}-{}",uid,e.what());
                }
                return res;
            }
        private:
            std::shared_ptr<odb::core::database> _db;
    };
}
