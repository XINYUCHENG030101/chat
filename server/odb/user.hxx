#pragma once 
#include <string> 
#include <cstddef>
#include<odb/core.hxx>
#include<odb/nullable.hxx>
namespace yu
{
    #pragma db object table("user")
    class user
    {
        public:
            user(){}
            user(const std::string& uid,const std::string& nickname,const std::string& password)
                :_user_id(uid)
                ,_nickname(nickname)
                ,_password(password)
            {}
            user(const std::string& uid,const std::string& phone)
                :_user_id(uid)
                ,_nickname(uid)
                ,_phone(phone)
            {}
            void user_id(const std::string user_id)
            {
                _user_id = user_id;
            }
            std::string user_id(){return _user_id;}

            std::string nickname() 
            {
                if(_nickname)
                    return *_nickname; 
                return std::string();
            }
            void nickname(const std::string& name){_nickname = name;}

            std::string description()
            {
                if(!_description)
                    return std::string();
                return *_description;
            }
            void description(const std::string& val){_description = val;}

            std::string phone()
            {
                if(!_phone)
                    return std::string();
                return *_phone;
            }
            void phone(const std::string& val){_phone = val;}

            std::string password()
            {
                if(!_password)
                    return std::string();
                return *_password;
            }
            void password(std::string& val){_password= val;}

            std::string avatar_id()
            {
                if(!_avatar_id)
                    return std::string();
                return *_avatar_id;
            }
            void avatar_id(const std::string& val){_avatar_id = val;}

        private:
            friend class odb::access;
            #pragma db id auto
            unsigned long _id;
            #pragma db type("varchar(64)") index unique
            std::string _user_id;
            #pragma db type("varchar(64)") index unique
            odb::nullable<std::string> _nickname;
            odb::nullable<std::string> _description;
            #pragma db type("varchar(64)")
            odb::nullable<std::string> _password;
            #pragma db type("varchar(64)") index unique
            odb::nullable<std::string> _phone;
            #pragma db type("varchar(64)")
            odb::nullable<std::string> _avatar_id;
    };
}
//odb -d mysql --std c++11 --generate-query --generate-schema --profile boost/date-time person.hxx