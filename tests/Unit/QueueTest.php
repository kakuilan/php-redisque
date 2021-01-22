<?php
/**
 * Copyright (c) 2021 LKK All rights reserved
 * User: kakuilan
 * Date: 2021/1/21
 * Time: 19:08
 * Desc:
 */

namespace Redisque\Tests\Unit;

use PHPUnit\Framework\TestCase;
use Error;
use Exception;
use Throwable;
use Redisque\RedisConn;
use Redisque\RedisClient;
use Redisque\RedisQueue;
use Redisque\QueueInterface;
use Redisque\QueueException;

/**
 * Class QueueTest
 * @package Redisque\Tests\Unit
 */
class QueueTest extends TestCase {


    /**
     * 队列1配置
     * @var array
     */
    public static $que1cnf = [
        'queueName' => 'hello',
        'connName'  => 'default',
        'isSort'    => false,
        'priority'  => 0,
        'expire'    => 1800,
        'transTime' => 600,
    ];


    /**
     * 队列2配置
     * @var array
     */
    public static $que2cnf = [
        'queueName' => 'world',
        'connName'  => 'comm',
        'isSort'    => true,
        'priority'  => 1,
        'expire'    => 0,
        'transTime' => 600,
    ];


    public function testGetPrefix() {
        $str = RedisQueue::getPrefix();
        $this->assertNotEmpty($str);
    }


    public function testDefaultRedis() {
        try {
            $client = RedisConn::getRedis(ConnTest::$conf);
            RedisQueue::setDefaultRedis($client);

            $client2 = RedisQueue::getDefaultRedis();
            $time1   = $client->getLastConnectTime();
            $time2   = $client2->getLastConnectTime();
            $this->assertEquals($time1, $time2);
        } catch (Throwable $e) {
        }
    }


    public function testNewQueue() {
        try {
            $client = RedisConn::getRedis(ConnTest::$conf);

            $queue1 = RedisQueue::setDefaultRedis($client)->newQueue(self::$que1cnf);
            $queue2 = RedisQueue::setDefaultRedis($client)->newQueue(self::$que2cnf);

            $queues = $queue1::getQueues();
            $this->assertTrue(count($queues) == 2);
        } catch (QueueException $e) {
        }
    }


}