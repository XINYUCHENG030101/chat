#include "../common/icsearch.hpp"
#include "user.hxx"
#include "message.hxx"

namespace yu
{
    class ESFactory
    {
        public:
            static std::shared_ptr<elasticlient::Client> create(const std::vector<std::string> &host_list)
            {
                auto client = std::make_shared<elasticlient::Client>(host_list);
                return client;
            }
    };
    class ESuser
    {
        public:
            using ptr = std::shared_ptr<ESuser>;
            ESuser(std::shared_ptr<elasticlient::Client> es_client)
            :_es_client(es_client)
            {}
            bool creatindex()
            {
                bool index = ESIndex(_es_client,"user")
                            .append("user_id","keyword","standard",true)
                            .append("nickname")
                            .append("phone", "keyword", "standard", true)
                            .append("description", "text", "standard", false)
                            .append("avatar_id", "keyword", "standard", false)
                            .create();
                if(index == false)
                {
                    LOG_INFO("用户信息索引创建失败");
                    return false;
                }
                LOG_INFO("用户信息索引创建成功");
                return true;
            }
            bool appenddata
            (
                const std::string &id,
                const std::string &nickname,
                const std::string &phone,
                const std::string &description,
                const std::string &avatar_id
            )
            {
                bool ret = ESInsert(_es_client,"user")
                            .append("user_id",id)
                            .append("nickname",nickname)
                            .append("phone",phone)
                            .append("description",description)
                            .append("avatar_id",avatar_id)
                            .insert(id);
                if(ret == false)
                {
                    LOG_ERROR("用户数据插入/更新失败");
                    return false;
                }
                LOG_INFO("用户数据插入/更新成功");
                return true;
            }
            std::vector<user> search(const std::string &key,const std::vector<std::string> &uid_list)
            {
                std::vector<user> res;
                Json::Value json_user = ESSearch(_es_client,"user")
                     .append_should_match("phone.keyword",key)
                     .append_should_match("user_id.keyword",key)
                     .append_should_match("nickname",key)
                     .append_must_not_terms("user_id.keyword",uid_list)
                     .search();
                if (json_user.isArray() == false) {
                    LOG_ERROR("用户搜索结果为空，或者结果不是数组类型");
                    return res;
                }
                int sz = json_user.size();
                LOG_DEBUG("检索结果条目数量：{}", sz);
                for (int i = 0; i < sz; i++) 
                {
                    user user;
                    user.user_id(json_user[i]["_source"]["user_id"].asString());
                    user.nickname(json_user[i]["_source"]["nickname"].asString());
                    user.description(json_user[i]["_source"]["description"].asString());
                    user.phone(json_user[i]["_source"]["phone"].asString());
                    user.avatar_id(json_user[i]["_source"]["avatar_id"].asString());
                    res.push_back(user);
                }
                return res;
            }
        private:
            std::shared_ptr<elasticlient::Client> _es_client;
    };
    class ESMessage
    {
        public:
            using ptr = std::shared_ptr<ESMessage>;
            ESMessage(const std::shared_ptr<elasticlient::Client>& es_client)
            :_es_client(es_client)
            {}
            bool creatindex()
            {
                bool index = ESIndex(_es_client,"message")
                            .append("user_id","keyword","standard",false)
                            .append("message_id","keyword", "standard", false)
                            .append("create_time", "long", "standard", false)
                            .append("content")
                            .append("session_id","keyword", "standard", true)
                            .create();
                if(index == false)
                {
                    LOG_INFO("消息信息索引创建失败");
                    return false;
                }
                LOG_INFO("消息信息索引创建成功");
                return true;
            }
             bool appenddata
            (
                const std::string &id,
                const std::string &message_id,
                const long &create_time,
                const std::string &content,
                const std::string &session_id
            )
            {
                bool ret = ESInsert(_es_client,"message")
                            .append("message_id",message_id)
                            .append("user_id",id)
                            .append("create_time",create_time)
                            .append("content",content)
                            .append("session_id",session_id)
                            .insert(message_id);
                if(ret == false)
                {
                    LOG_ERROR("消息数据插入/更新失败");
                    return false;
                }
                LOG_INFO("消息数据插入/更新成功");
                return true;
            }
            bool remove(std::string& mid)
            {
                bool ret = ESRemove(_es_client, "message").remove(mid);
                if (ret == false) 
                {
                    LOG_ERROR("消息数据删除失败!");
                    return false;
                }
                LOG_INFO("消息数据删除成功!");
                return true;
            }
            std::vector<yu::Message> search(const std::string& key,const std::string& ssid)
            {
                std::vector<yu::Message> res;
                Json::Value json_user = ESSearch(_es_client,"message")
                    .append_must_term("session_id.keyword",ssid)
                    .append_must_match("content",key)
                    .search();
                if (json_user.isArray() == false) {
                    LOG_ERROR("用户搜索结果为空，或者结果不是数组类型");
                    return res;
                }
                int sz = json_user.size();
                LOG_DEBUG("检索结果条目数量：{}", sz);
                for (int i = 0; i < sz; i++) 
                {
                    yu::Message message;
                    message.message_id(json_user[i]["_source"]["message_id"].asString());
                    message.user_id(json_user[i]["_source"]["user_id"].asString());
                    boost::posix_time::ptime ctime(boost::posix_time::from_time_t(
                        json_user[i]["_source"]["create_time"].asInt64()));
                    message.create_time(ctime);
                    message.content(json_user[i]["_source"]["content"].asString());
                    message.session_id(json_user[i]["_source"]["session_id"].asString());
                    res.push_back(message);
                }
                return res;
            }
        private:
            std::shared_ptr<elasticlient::Client> _es_client;
    };
}