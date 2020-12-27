<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2020/12/8
 * Time: 11:40
 * Desc: 队列接口
 */

namespace Redisque;

use Redis;
use Throwable;

/**
 * Interface QueueInterface
 * @package Redisque
 */
interface QueueInterface {


    /**
     * 获取key的前缀
     * @return string
     */
    public static function getPrefix(): string;


    /**
     * 设置默认的redis客户端连接
     * @param Redis $client 客户端
     * @return static
     */
    public static function setDefaultRedis(Redis $client): self;


    /**
     * 获取默认的redis客户端连接
     * @return Redis
     * @throws Throwable
     */
    public static function getDefaultRedis(): Redis;


    /**
     * 获取所有队列名
     * @param bool $readCache 是否从缓存读取
     * @param null|mixed $redis Redis客户端对象
     * @return array
     */
    public static function getQueues(bool $readCache = true, $redis = null): array;


    /**
     * 统计队列数
     * @param bool $readCache 是否从缓存读取
     * @param null|mixed $redis Redis客户端对象
     * @return int
     */
    public static function countQueues(bool $readCache = true, $redis = null): int;


    /**
     * 检查队列是否存在
     * @param string $queueName 队列名
     * @param null|mixed $redis Redis客户端对象
     * @return bool
     */
    public static function queueExists(string $queueName, $redis = null): bool;


    /**
     * 获取哈希表(存储全部队列名)的键名
     * @return string
     */
    public static function getTableKey(): string;


    /**
     * 获取(消费)队列的键名
     * @param string $queueName 队列名
     * @param null|mixed $redis Redis客户端对象
     * @return string
     */
    public static function getQueueKey(string $queueName, $redis = null): string;


    /**
     * 获取中转哈希表(存储中转消息)的键名
     * @param int $priority 是否优先
     * @return string
     */
    public static function getTransTableKey(int $priority): string;


    /**
     * 获取中转队列(存储中转消息key)的键名
     * @param int $priority
     * @return string
     */
    public static function getTransQueueKey(int $priority): string;


    /**
     * 获取操作锁
     * @param string $operation 操作名
     * @param mixed $dataId 数据ID
     * @param int $operateUid 当前操作者UID
     * @param int $ttl 有效期
     * @param null|mixed $redis Redis客户端对象
     * @return int 获取到锁的UID:>0时为本身;<=0时为他人
     */
    public static function getLockOperate(string $operation, $dataId, int $operateUid, int $ttl = 60, $redis = null): int;


    /**
     * 解锁操作
     * @param string $operation 操作名
     * @param mixed $dataId 数据ID
     * @param null|mixed $redis Redis客户端对象
     * @return bool
     */
    public static function unlockOperate(string $operation, $dataId, $redis = null): bool;


    /**
     * 新建队列/设置队列
     * @param array $conf 队列配置
     * @return static
     * @throws Throwable
     */
    public function newQueue(array $conf): self;


    /**
     * 获取redis客户端连接
     * @param string $connName 连接名
     * @return Redis
     * @throws Throwable
     */
    public function getRedisClient(string $connName = ''): Redis;


    /**
     * 获取队列信息
     * @param string $queueName 队列名
     * @return object
     */
    public function getQueueInfo(string $queueName = ''): object;


    /**
     * 添加队列名(到哈希表)
     * @param string $queueName 队列名
     * @param string $sortType 排序类型
     * @param int $priority 是否优先
     * @return bool
     */
    public function addQueueName(string $queueName, string $sortType, int $priority): bool;


    /**
     * 消息是否经过包装
     * @param array $msg
     * @return bool
     */
    public static function isWraped(array $msg): bool;


    /**
     * (有序队列的)消息包装
     * @param array $msg 原始消息
     * @param int $weight 权重,0~99,值越大在队列中越排前,仅对有序队列起作用
     * @return array
     */
    public static function wrapMsg(array $msg, int $weight): array;


    /**
     * 消息反包装
     * @param array $msg 经包装的消息
     * @return array
     */
    public static function unwrapMsg(array $msg): array;


    /**
     * 消息打包
     * @param array $msg
     * @return string
     */
    public function pack(array $msg): string;


    /**
     * 消息解包
     * @param string $msg
     * @return array
     */
    public function unpack(string $msg): array;


    /**
     * 队列头压入一个消息
     * @param array $msg
     * @return bool
     */
    public function add(array $msg): bool;


    /**
     * 队列头压入多个个消息
     * @param array ...$msgs
     * @return bool
     */
    public function addMulti(array ...$msgs): bool;


    /**
     * 队列尾压入一个消息
     * @param array $msg
     * @return bool
     */
    public function push(array $msg): bool;


    /**
     * 队列尾压入多个消息
     * @param array ...$msgs
     * @return bool
     */
    public function pushMulti(array ...$msgs): bool;


    /**
     * 队列头移出元素
     * @return mixed
     */
    public function shift();


    /**
     * 队列尾移出元素
     * @return mixed
     */
    public function pop();


    /**
     * 将中转消息重新加入相应的处理队列
     * @param int $transType 中转队列类型:0非优先队列中转,1优先队列中转
     * @param string $uniqueCode 机器唯一码
     * @return int
     */
    public function transMsgReadd2Queue(int $transType, string $uniqueCode = ''): int;


    /**
     * 获取单个消息的中转key
     * @param array $msg
     * @return string
     */
    public function getMsgToTransKey(array $msg): string;


    /**
     * 根据中转key获取单个消息
     * @param string $key
     * @return array
     */
    public function getMsgByTransKey(string $key): array;


    /**
     * 获取多个消息的中转key
     * @param array ...$msg
     * @return array
     */
    public function getMsgsToTransKeys(array ...$msg): array;


    /**
     * 根据中转key获取多个消息
     * @param string ...$key
     * @return array
     */
    public function getMsgsByTransKeys(string ...$key): array;


    /**
     * 将消息加入中转队列
     * @param array $msg
     * @return bool
     */
    public function transfer(array $msg): bool;


    /**
     * 消息确认(处理完毕后向队列确认,成功则从中转队列移除;失败则重新加入任务队列;若无确认,消息重新入栈)
     * @param bool $ok 处理结果:true成功,false失败
     * @param mixed $msg 消息或该消息的中转key
     * @return bool
     */
    public function confirm(bool $ok, $msg): bool;


    /**
     * 消息批量确认
     * @param bool $ok 处理结果:true成功,false失败
     * @param mixed ...$msgs 消息或该消息的中转key
     * @return int
     */
    public function confirmMulti(bool $ok, ...$msgs): int;


    /**
     * 获取队列长度
     * @param string $queueName 队列名
     * @return int
     */
    public function len(string $queueName = ''): int;


    /**
     * 清空队列
     * @param string $queueName 队列名
     * @return bool
     */
    public function clear(string $queueName = ''): bool;


    /**
     * 删除队列
     * @param string $queueName 队列名
     * @return bool
     */
    public function delete(string $queueName = ''): bool;


}