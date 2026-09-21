#pragma once
#include <cstddef>
#include <odb/core.hxx>
#include <odb/nullable.hxx>
#include <string>
namespace yu 
{
  #pragma db object table("relation")
  class Relation 
  {
    public:
    Relation() {}
    Relation(const std::string &user_id, const std::string &friend_id)
        : _user_id(user_id), _friend_id(friend_id) {}
    std::string user_id() { return _user_id; }
    std::string friend_id() { return _friend_id; }
    void friend_id(const std::string &val) { _friend_id = val; }
    void user_id(const std::string &val) { _user_id = val; }

  private:
    friend class odb::access;
    #pragma db id auto
    unsigned long _id;
    #pragma db type("varchar(64)") index
    std::string _user_id;
    #pragma db type("varchar(64)")
    std::string _friend_id;
};
} // namespace yu