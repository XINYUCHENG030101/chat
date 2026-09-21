#pragma once
#include "message-odb.hxx"
#include "message.hxx"
#include "mysql.hpp"

namespace yu {
class Messagetable {
public:
  using ptr = std::shared_ptr<Messagetable>;
  Messagetable(const std::shared_ptr<odb::core::database> &db) : _db(db) {}
  // 新增消息
  bool insert(Message &message) {
    try {
      // 获取事务对象开启事务
      odb::transaction trans(_db->begin());
      _db->persist(message);
      // 5. 提交事务
      trans.commit();
    } catch (std::exception &e) {
      LOG_ERROR("新增消息出错:{}{}", message.content(), e.what());
      return false;
    }
    return true;
  }
  // 通过消息ID 删除会话消息
  bool remove(const std::string &ssid) {
    try {
      odb::transaction trans(_db->begin());
      typedef odb::query<Message> query;
      typedef odb::result<Message> result;
      _db->erase_query<Message>(query::session_id == ssid);
      // 5. 提交事务
      trans.commit();
    } catch (std::exception &e) {
      LOG_ERROR("删除单个成员失败{}-{}", ssid, e.what());
      return false;
    }
    return true;
  }
  // 通过会话ID，消息数量，获取最近的N 条消息（逆序+limit 即可）
  std::vector<Message> recent(const std::string &ssid, int count) {
    std::vector<Message> res;
    try {
      odb::transaction trans(_db->begin());
      typedef odb::query<Message> query;
      typedef odb::result<Message> result;
      std::stringstream ss;
      // select * from message where session_id = ssid order by create_time desc
      // limit count
      ss << "session_id = '" << ssid << "'";
      ss << " order by create_time desc limit " << count;
      result r(_db->query<Message>(ss.str()));
      for (auto it = r.begin(); it != r.end(); ++it) {
        res.push_back(*it);
      }
      std::reverse(res.begin(), res.end());
      trans.commit();
    } catch (const std::exception &e) {
      LOG_ERROR("获取最近消息失败{}-{}-{}", ssid, count, e.what());
    }
    return res;
  }
  // 通过会话ID，时间范围，获取指定时间段之内的消息，并按时间进行排序
  std::vector<Message> range(const std::string &ssid,
                             boost::posix_time::ptime &stime,
                             boost::posix_time::ptime &etime) {
    std::vector<Message> res;
    try {
      odb::transaction trans(_db->begin());
      typedef odb::query<Message> query;
      typedef odb::result<Message> result;
      result r(_db->query<Message>(query::session_id == ssid &&
                                   query::create_time >= stime &&
                                   query::create_time <= etime));
      for (auto it = r.begin(); it != r.end(); it++) {
        res.push_back(*it);
      }
      trans.commit();
    } catch (std::exception &e) {
      LOG_ERROR("获取消息失败{} {}-{} {}", ssid,
                boost::posix_time::to_simple_string(stime),
                boost::posix_time::to_simple_string(etime), e.what());
    }
    return res;
  }

private:
  std::shared_ptr<odb::core::database> _db;
};
} // namespace yu