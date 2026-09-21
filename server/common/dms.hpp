// EmailVerificationSender.hpp
#ifndef EMAIL_VERIFICATION_SENDER_HPP
#define EMAIL_VERIFICATION_SENDER_HPP
#include<iostream>
#include<string>
#include<curl/curl.h>
#include<string.h>
#include<cstdlib>
#include<memory>
#include"logger.hpp"
namespace yu
{
    class EmailVerificationSender 
    {
        private:
            std::string smtpServer;   // 例如 "smtps://smtp.qq.com:465"
            std::string fromEmail;    // 发件人邮箱地址
            std::string authCode;     // 发件人邮箱授权码，非登录密码
            std::string senderName;   // 发件人显示名称（可选）

            // libcurl的回调函数，用于读取邮件数据
            static size_t payloadSource(void *ptr, size_t size, size_t nmemb, void *userp);
        public:
            using ptr = std::shared_ptr<EmailVerificationSender>;
            // 构造函数：初始化发件人信息和SMTP服务器
            EmailVerificationSender(const std::string& server = "smtps://smtp.qq.com:465" ,
                                    const std::string& email = "",
                                    const std::string& code = "",
                                    const std::string& name = "系统验证码服务");

            // 核心接口：发送验证码到指定邮箱
            bool sendVerificationCode(const std::string& recipientEmail, const std::string& verificationCode);
        };

        // 注意：.hpp文件包含实现，因此函数定义也在下面
        EmailVerificationSender::EmailVerificationSender(const std::string& server,
                                                        const std::string& email,
                                                        const std::string& code,
                                                        const std::string& name)
            : smtpServer(server), fromEmail(email), authCode(code), senderName(name) {
            curl_global_init(CURL_GLOBAL_DEFAULT);
        }

        size_t EmailVerificationSender::payloadSource(void *ptr, size_t size, size_t nmemb, void *userp) {
            // 实现细节：将构建好的邮件内容字符串通过此回调提供给libcurl
            std::string* payload = static_cast<std::string*>(userp);
            if(size * nmemb < 1 || payload->empty()) return 0;
            size_t copyLen = std::min(size * nmemb, payload->size());
            memcpy(ptr, payload->c_str(), copyLen);
            payload->erase(0, copyLen);
            return copyLen;
        }

        // 修改dms.hpp中的sendVerificationCode函数
    bool EmailVerificationSender::sendVerificationCode(const std::string& recipientEmail, 
                                                    const std::string& verificationCode) 
    {
        CURL* curl = curl_easy_init();
        if(!curl) 
            return false;
        
        struct curl_slist* recipients = nullptr;
        CURLcode res = CURLE_OK;
        bool success = false;
        
        // 1. 构建邮件内容
        std::string emailPayload =
            "From: \"" + senderName + "\" <" + fromEmail + ">\r\n"
            "To: <" + recipientEmail + ">\r\n"
            "Subject: 您的验证码\r\n"
            "Content-Type: text/plain; charset=utf-8\r\n"
            "\r\n"
            "您的验证码是: " + verificationCode + "\r\n"
            "该验证码5分钟内有效，请勿泄露给他人。\r\n"
            "\r\n.\r\n";
        
        std::string payloadCopy = emailPayload;
        
        // 2. 配置libcurl选项
        curl_easy_setopt(curl, CURLOPT_URL, smtpServer.c_str());
        curl_easy_setopt(curl, CURLOPT_MAIL_FROM, fromEmail.c_str());
        
        recipients = curl_slist_append(recipients, recipientEmail.c_str());
        curl_easy_setopt(curl, CURLOPT_MAIL_RCPT, recipients);
        
        curl_easy_setopt(curl, CURLOPT_USERNAME, fromEmail.c_str());
        curl_easy_setopt(curl, CURLOPT_PASSWORD, authCode.c_str());
        curl_easy_setopt(curl, CURLOPT_USE_SSL, CURLUSESSL_ALL);
        
        // 重要：强制使用LOGIN认证方式
        curl_easy_setopt(curl, CURLOPT_LOGIN_OPTIONS, "AUTH=LOGIN");
        
        curl_easy_setopt(curl, CURLOPT_READFUNCTION, payloadSource);
        curl_easy_setopt(curl, CURLOPT_READDATA, &payloadCopy);
        curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
        curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, 1L);       // 最重要的优化
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);          // 设置超时
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 10L);   // 连接超时
        // 启用详细日志
        curl_easy_setopt(curl, CURLOPT_VERBOSE, 0L);
        
        // 3. 执行发送
        res = curl_easy_perform(curl);
        if(res == CURLE_OK) 
        {
            success = true;
        } else 
        {
            LOG_ERROR("验证码发送失败{}",curl_easy_strerror(res));
        }
        
        // 4. 清理资源
        curl_slist_free_all(recipients);
        curl_easy_cleanup(curl);
        
        return success;
    }

}
#endif // EMAIL_VERIFICATION_SENDER_HPP
