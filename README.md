技术栈：C++ / brpc / Protobuf / etcd / Redis / MySQL(ODB) / Elasticsearch / RabbitMQ / WebSocket / HTTP

基于 brpc + Protobuf 拆分为网关、用户、好友、消息存储、消息转发、文件、语音识别等 7 个微服务，通过 etcd 完成服务注册与发现。

网关采用 HTTP（业务请求）+ WebSocket（实时推送）双通道；登录会话、在线状态、验证码等热点数据落 Redis。

消息转发服务负责会话成员解析与在线推送，并通过 RabbitMQ 异步落库，实现发送与持久化解耦。

支持文本/图片/文件/语音等多类型消息；消息与用户检索接入 Elasticsearch；语音识别对接百度ASR；好友关系支持申请审批、单聊与群聊会话管理。
