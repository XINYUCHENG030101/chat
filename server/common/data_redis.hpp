#include <sw/redis++/redis.h>
#include <iostream>

namespace yu
{
    class RedisFactory
    {
        public:
            static std::shared_ptr<sw::redis::Redis> create
            (
                const std::string &host,
                const int &port,
                const int &db,
                bool keep_alive    
            )
            {
                sw::redis::ConnectionOptions opts;
                opts.host = host;
                opts.port = port;
                opts.db = db;
                opts.keep_alive = keep_alive;
                auto client = std::make_shared<sw::redis::Redis>(opts);
                return client;
            }
    };
    class session
    {
        public:
            using ptr = std::shared_ptr<session>;
            session(const std::shared_ptr<sw::redis::Redis> &redis_client)
            :_redis_client(redis_client)
            {}
            void append(const std::string &ssid,const std::string &uid)
            {
                _redis_client->set(ssid,uid);
            }
            void remove(const std::string &ssid)
            {
                _redis_client->del(ssid);
            }
            sw::redis::OptionalString uid(const std::string &ssid)
            {
                return  _redis_client->get(ssid);
            }
        private:
            std::shared_ptr<sw::redis::Redis> _redis_client;
    };
    class status
    {
        public:
            using ptr = std::shared_ptr<status>;
            status(const std::shared_ptr<sw::redis::Redis> &redis_client)
            :_redis_client(redis_client)
            {}
            void append(const std::string &uid)
            {
                _redis_client->set(uid,"");
            }
            void remove(const std::string &uid)
            {
                _redis_client->del(uid);
            }
            bool exist(const std::string &uid)
            {
                auto ret = _redis_client->get(uid);
                if(ret)
                    return true;
                return false;
            }
        private:
            std::shared_ptr<sw::redis::Redis> _redis_client;
    };
    class code
    {
        public:
            using ptr = std::shared_ptr<code>;
            code(const std::shared_ptr<sw::redis::Redis> &redis_client)
            :_redis_client(redis_client)
            {}
            void append(const std::string &code,const std::string &cid,const std::chrono::milliseconds &t = std::chrono::milliseconds(300000) )
            {
                _redis_client->set(cid,code,t);
            }
            void remove(const std::string &cid)
            {
                _redis_client->del(cid);
            }
            sw::redis::OptionalString get_code(const std::string &cid)
            {
                return  _redis_client->get(cid);
            }
        private:
            std::shared_ptr<sw::redis::Redis> _redis_client;
    };
}