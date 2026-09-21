#pragma once
#include "mysql.hpp"
#include "user.hxx"
#include "user-odb.hxx"
namespace yu
{
    class Usertable
    {
        public:
            using ptr = std::shared_ptr<Usertable>;
            Usertable(const std::shared_ptr<odb::core::database> &db)
            :_db(db){}
            bool insert(const std::shared_ptr<user> &user)
            {
                try 
                {
                    //获取事务对象开启事务
                    odb::transaction trans(_db->begin());
                    _db->persist(*user);
                    //5. 提交事务
                    trans.commit();
                }
                catch (std::exception &e) 
                {
                    LOG_ERROR("插入数据出错:{}{}",user->nickname(),e.what());
                    return false;
                }
                return true;
            }
            bool update(const std::shared_ptr<user> &user)
            {
                try
                {
                    odb::transaction trans(_db->begin());
                    _db->update(*user);
                    trans.commit();

                }
                catch(const std::exception& e)
                {
                    LOG_ERROR("更新数据出错:{}{}",user->nickname(),e.what());
                    return false;
                }
                return true;
                
            }
            std::shared_ptr<user> select_by_nickname(const std::string &nickname)
            {
                std::shared_ptr<user> res;
                try
                {
                    odb::transaction trans(_db->begin());
                    typedef odb::query<user> query;
                    typedef odb::result<user> result;
                    res.reset(_db->query_one<user>(query::nickname == nickname));
                    trans.commit();
                }
                catch(const std::exception& e)
                {
                    LOG_ERROR("通过昵称查找用户失败:{}:{}",nickname,e.what());
                }
                return res;
            }
            std::shared_ptr<user> select_by_phone(const std::string &phone)
            {
                std::shared_ptr<user> res;
                try
                {
                    odb::transaction trans(_db->begin());
                    typedef odb::query<user> query;
                    typedef odb::result<user> result;
                    res.reset(_db->query_one<user>(query::phone == phone));
                    trans.commit();
                }
                catch(const std::exception& e)
                {
                    LOG_ERROR("通过电话查找用户失败:{}:{}",phone,e.what());
                }
                return res;
            }
            std::shared_ptr<user> select_by_id(const std::string &id)
            {
                std::shared_ptr<user> res;
                try
                {
                    odb::transaction trans(_db->begin());
                    typedef odb::query<user> query;
                    typedef odb::result<user> result;
                    res.reset(_db->query_one<user>(query::user_id == id));
                    trans.commit();
                }
                catch(const std::exception& e)
                {
                    LOG_ERROR("通过用户id查找用户失败:{}:{}",id,e.what());
                }
                return res;
            }
            std::vector<user> select_multi_users(std::vector<std::string> &id_lists)
            {
                std::vector<user> res;
                try
                {
                    odb::transaction trans(_db->begin());
                    typedef odb::query<user> query;
                    typedef odb::result<user> result;
                    std::stringstream ss;
                    ss<<"user_id in (";
                    for(auto id : id_lists)
                    {
                        ss<<"'"<<id<<"',";
                    }
                    std::string condition = ss.str();
                    condition.pop_back();
                    condition+=")";
                    result r(_db->query<user>(condition));
                    for (auto it = r.begin(); it != r.end(); ++it) 
                    {
                        res.push_back(*it);
                    }
                    trans.commit();
                }
                catch(const std::exception& e)
                {
                    LOG_ERROR("批量查询用户失败{}",e.what());
                }
                return res;
            }
        private:
            std::shared_ptr<odb::core::database> _db;
    };
}