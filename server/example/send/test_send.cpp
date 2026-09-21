#include "dms.hpp"
#include <iostream>


using namespace yu;
int main() 
{
    // 1. 初始化发送器（配置在程序生命周期内通常只需一次）

    
    yu::EmailVerificationSender::ptr sender = std::make_shared<EmailVerificationSender>();
    // 2. 生成验证码（此处简化为固定值，实际应随机生成[1](@ref)[6](@ref)）
    std::string code = "123456";
    std::string recipient = "962606552@qq.com";

    // 3. 调用接口发送
    if(sender->sendVerificationCode(recipient, code)) {
         std::cout << "验证码发送成功！" << std::endl;
    } else {
        std::cout << "验证码发送失败，请检查网络或配置。" << std::endl;
    }
    return 0;
}
