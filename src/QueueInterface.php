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
use RedisException;
use Throwable;

/**
 * Interface QueueInterface
 * @package Redisque
 */
interface QueueInterface {


    /**
     * 获取key前缀
     * @return string
     */
    public static function getPrefix(): string;


    /**
     * 获取操作锁
     * @param string $operation 操作名
     * @param int $dataId 数据ID
     * @param int $operateUid 当前操作者UID
     * @param int $ttl 有效期
     * @param null $redis Redis客户端对象
     * @return int 获取到锁的UID:>0时为本身;<=0时为他人
     */
    public static function getLockOperate(string $operation, int $dataId, int $operateUid, int $ttl = 60, $redis = null): int;


    /**
     * 解锁操作
     * @param string $operation 操作名
     * @param int $dataId 数据ID
     * @param null $redis Redis客户端对象
     * @return bool
     */
    public static function unlockOperate(string $operation, int $dataId, $redis = null): bool;


    /**
     * 新建队列
     * @param array $conf 队列配置
     * @return $this
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
     * 队列头压入一个消息
     * @param array $item
     * @return bool
     */
    public function add(array $item): bool;


    /**
     * 队列头压入多个个消息
     * @param array ...$items
     * @return bool
     */
    public function addMulti(array ...$items): bool;


    /**
     * 队列尾压入一个消息
     * @param array $item
     * @return bool
     */
    public function push(array $item): bool;


    /**
     * 队列尾压入多个消息
     * @param array ...$items
     * @return bool
     */
    public function pushMulti(array ...$items): bool;


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
     * 将消息加入中转队列
     * @param array $item
     * @return bool
     */
    public function transfer(array $item): bool;


    /**
     * 消息确认(处理完毕后向队列确认,成功则从中转队列移除;失败则重新加入任务队列;若无确认,消息重新入栈)
     * @param mixed $item 消息或该消息的中转key
     * @param bool $ok 处理结果:true成功,false失败
     * @return bool
     */
    public function confirm($item, bool $ok = true): bool;


    /**
     * 将中转消息重新加入相应的处理队列
     * @param int $transType 中转队列类型:0非优先队列中转,1优先队列中转
     * @param string $uniqueCode 机器唯一码
     * @return int
     */
    public function transMsgReadd2Queue(int $transType, string $uniqueCode = ''): int;


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