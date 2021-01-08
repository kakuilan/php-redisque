<?php
/**
 * Copyright (c) 2020 LKK All rights reserved
 * User: kakuilan
 * Date: 2020/12/14
 * Time: 11:43
 * Desc: Redis客户端
 */

namespace Redisque;

use Redis;


/**
 * Class RedisClient
 * @package Redisque
 */
class RedisClient extends Redis {


    /**
     * 上次连接时间
     * @var int
     */
    protected $lastConnectTime = 0;


    /**
     * 设置上次连接时间
     * @param int $time
     */
    public function setLastConnectTime(int $time): void {
        if ($time > 0) {
            $this->lastConnectTime = $time;
        }
    }


    /**
     * 获取上次连接时间
     * @return int
     */
    public function getLastConnectTime(): int {
        return $this->lastConnectTime;
    }


}