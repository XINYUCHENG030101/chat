#pragma once 
#include <string> 
#include <cstddef>
#include<odb/core.hxx>
#include<odb/nullable.hxx>
namespace yu
{
    #pragma db object table("SessionMember")
    class ChatSessionMember
    {   
        public:
            ChatSessionMember(){}
            ChatSessionMember(const std::string& user_id,const std::string& session_id)
            :_user_id(user_id)
            ,_session_id(session_id)
            {}
            void user_id(const std::string user_id)
            {
                _user_id = user_id;
            }
            std::string user_id(){return _user_id;}
            void session_id(const std::string session_id)
            {
                _session_id = session_id;
            }
            std::string session_id(){return _session_id;}
        private:
            friend class odb::access;
            #pragma db id auto
            unsigned long _id;
            #pragma db type("varchar(64)") 
            std::string _user_id;
            #pragma db type("varchar(64)") index
            std::string _session_id;//会话id
    };
}