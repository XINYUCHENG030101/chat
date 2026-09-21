#include<spdlog/spdlog.h>
#include<spdlog/sinks/stdout_color_sinks.h>
#include<spdlog/sinks/basic_file_sink.h>
#include<spdlog/async.h>
#include<iostream>
int main()
{
    //设置全局刷新策略
    spdlog::flush_every(std::chrono::seconds(1));
    spdlog::flush_on(spdlog::level::level_enum::debug); 
    //设置全局输出等级

    //创建同步日志器
    //设置日志器的刷新策略和输出等级（全局和日志器随意选择一个即可）
    //进行简单的日志输出
    return 0;
}