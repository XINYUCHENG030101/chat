#pragma once
#include<spdlog/spdlog.h>
#include<spdlog/sinks/stdout_color_sinks.h>
#include<spdlog/sinks/basic_file_sink.h>
#include<spdlog/async.h>
#include<iostream>

namespace yu
{
    std::shared_ptr<spdlog::logger> g_logger;
    //mode -运行模式 true发布模式  false调试模式
    void init_logger(bool mode,std::string& file_name,int32_t level)
    {
        //如果是调试模式那么就选择标准输出
        //如果不是调试模式那么就输出到文件
        if(mode == false)
        {
            g_logger = spdlog::stdout_color_mt("default-logger");
            g_logger->set_level(spdlog::level::level_enum::trace);
            g_logger->flush_on(spdlog::level::level_enum::trace);
        }
        else
        {
            g_logger = spdlog::basic_logger_mt("default-logger",file_name);
            g_logger->set_level((spdlog::level::level_enum)level);
            g_logger->flush_on((spdlog::level::level_enum)level);
        }
        g_logger->set_pattern("[%n][%H:%M:%S][%t][%-8l]%v");
    }
    #define LOG_TRACE(format,...) yu::g_logger->trace(std::string("[{}:{}] ") + format, __FILE__, __LINE__, ##__VA_ARGS__)
    #define LOG_DEBUG(format,...) yu::g_logger->debug(std::string("[{}:{}]") + format, __FILE__, __LINE__,##__VA_ARGS__)
    #define LOG_INFO(format,...) yu::g_logger->info(std::string("[{}:{}]") + format, __FILE__, __LINE__,##__VA_ARGS__)
    #define LOG_WARN(format,...) yu::g_logger->warn(std::string("[{}:{}]") + format, __FILE__, __LINE__,##__VA_ARGS__)
    #define LOG_ERROR(format,...) yu::g_logger->error(std::string("[{}:{}]") + format, __FILE__, __LINE__,##__VA_ARGS__)
    #define LOG_FATAL(format,...) yu::g_logger->critical(std::string("[{}:{}]") + format, __FILE__, __LINE__,##__VA_ARGS__)

}
