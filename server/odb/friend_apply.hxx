#pragma once
#include <cstddef>
#include <odb/core.hxx>
#include <odb/nullable.hxx>
#include <string>
namespace yu
{
    #pragma db object table("friend_apply")
    class Friend_apply
    {
        public:
            Friend_apply(){}
            Friend_apply(const std::string &event_id, const std::string &user_id, const std::string &peer_id)
                : _event_id(event_id), _user_id(user_id), _peer_id(peer_id) {}
            std::string event_id() { return _event_id; }
            std::string user_id() { return _user_id; }
            std::string peer_id() { return _peer_id; }
            void event_id(const std::string &val) { _event_id = val; }
            void user_id(const std::string &val) { _user_id = val; }
            void peer_id(const std::string &val) { _peer_id = val; }
        private:
            friend class odb::access;
            #pragma db id auto
            unsigned long _id;
            #pragma db type("varchar(64)") index unique
            std::string _event_id;
            #pragma db type("varchar(64)") index
            std::string _user_id;
            #pragma db type("varchar(64)") index
            std::string _peer_id;

    };
}