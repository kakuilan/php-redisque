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

/**
 * Interface QueueInterface
 * @package Redisque
 */
interface QueueInterface {


    /**
     * 获取redis客户端连接
     * @param string $connName 连接名
     * @return Redis
     */
    public function getRedisClient(string $connName = ''): Redis;




}