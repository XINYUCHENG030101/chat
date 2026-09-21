#pragma once
#include <cstddef>
#include <odb/core.hxx>
#include <odb/nullable.hxx>
#include <string>
#include "ChatSessionMember.hxx"
namespace yu
{
    enum class Chatsessiontype
    {
        Single = 1,
        Group = 2,
    };
    #pragma db object table("chat_session")
    class Chat_session
    {
        public:
            Chat_session(){}
            Chat_session(const std::string &chat_session_id, 
                const std::string &chat_session_name, Chatsessiontype chat_session_type)
                : _chat_session_id(chat_session_id)
                , _chat_session_name(chat_session_name)
                , _chat_session_type(chat_session_type) {}
            std::string chat_session_id() { return _chat_session_id; }
            std::string chat_session_name() { return _chat_session_name; }
            Chatsessiontype chat_session_type() { return _chat_session_type; }

            void chat_session_id(const std::string &val) { _chat_session_id = val; }
            void chat_session_name(const std::string &val) { _chat_session_name = val; }
            void chat_session_type(Chatsessiontype val) { _chat_session_type = val; }
        private:
            friend class odb::access;
            #pragma db id auto
            unsigned long _id;
            #pragma db type("varchar(64)")index unique
            std::string _chat_session_id;
            #pragma db type("varchar(64)")
            std::string _chat_session_name;
            #pragma db type("tinyint(1)")
            Chatsessiontype _chat_session_type;
    };
    //查询条件唯一：css::chat_session_type==1&&csm1.user_id == uid && csm2.user_id != uid
    #pragma db view object(Chat_session = css)\
                    object(ChatSessionMember=csm1:csm1::_session_id == css::_chat_session_id)\
                    object(ChatSessionMember=csm2:csm2::_session_id == css::_chat_session_id)\
                    query((?))
    struct SingleChatSession
    {
        #pragma db column(css::_chat_session_id)
        std::string chat_session_id;
        #pragma db column(csm2::_user_id)
        std::string friend_id;
    };
    //查询条件唯一:css::chat_session_type==2&&csm1.user_id == uid 
    #pragma db view object(Chat_session=css)\
                    object(ChatSessionMember=csm1:csm1::_session_id == css::_chat_session_id)\
                    query((?))
    struct GroupChatSession
    {
        #pragma db column(css::_chat_session_id)
        std::string chat_session_id;
        #pragma db column(css::_chat_session_name)
        std::string chat_session_name;
    };
}
