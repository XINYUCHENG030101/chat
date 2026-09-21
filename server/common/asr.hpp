#pragma once
#include "../third/include/aip-cpp-sdk-4.16.7/speech.h"
#include "logger.hpp"

namespace yu
{
    class AsrClient
    {
        public:
        using ptr = std::shared_ptr<yu::AsrClient>;
        AsrClient(const std::string &app_id, const std::string& api_key,const std::string& secret_key)
        :_client(app_id,api_key,secret_key)
        {}
        std::string recognize(const std::string &file_content,std::string &err)
        {
            Json::Value result = _client.recognize(file_content,"pcm",16000,aip::null);
            if(result["err_no"].asInt()!=0)
            {
                LOG_ERROR("语音识别失败:{}",result["err_msg"].asString());
                err = result["err_msg"].asString();
                return std::string();
            }
            return result["result"][0].asString();
        }
        private:
            aip::Speech _client;
    };
}


