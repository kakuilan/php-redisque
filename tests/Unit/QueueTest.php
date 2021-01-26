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
use Kph\Helpers\StringHelper;
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
        'expire'    => 3600,
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
        'expire'    => 1800,
        'transTime' => 600,
    ];


    public function testGetPrefix() {
        $str = RedisQueue::getPrefix();
        $this->assertNotEmpty($str);
    }


    public function testDefaultRedis() {
        try {
            RedisQueue::getDefaultRedis();
            RedisQueue::getQueues();

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
            $this->assertTrue(2 === RedisQueue::countQueues());
            $this->assertTrue(RedisQueue::queueExists('hello'));
        } catch (QueueException $e) {
        }
    }


    public function testAdd() {
        try {
            $client = RedisConn::getRedis(ConnTest::$conf);
            $queue1 = RedisQueue::setDefaultRedis($client)->newQueue(self::$que1cnf);
            $queue2 = RedisQueue::setDefaultRedis($client)->newQueue(self::$que2cnf);

            $queue1->clear();
            $queue1->clear('world');

            for ($i = 0; $i < 500; $i++) {
                $item1 = [
                    'type' => 'add',
                    'name' => StringHelper::randString(4, 5),
                    'age'  => rand(1, 99),
                ];
                $item2 = [
                    'type'   => 'add',
                    'order'  => StringHelper::randSimple(32),
                    'status' => boolval(mt_rand(0, 1)),
                ];

                $queue1->add($item1, mt_rand(1, 99));
                $queue2->add($item2, mt_rand(1, 99));
            }

            $len1 = $queue1->len();
            $len2 = $queue2->len();
            $this->assertEquals(500, $len1);
            $this->assertEquals(500, $len2);
        } catch (QueueException $e) {
        }
    }


    public function testAddMulti() {
        try {
            $client = RedisConn::getRedis(ConnTest::$conf);
            $queue1 = RedisQueue::setDefaultRedis($client)->newQueue(self::$que1cnf);
            $queue2 = RedisQueue::setDefaultRedis($client)->newQueue(self::$que2cnf);
            $queue1->clear();
            $queue2->clear();

            for ($i = 0; $i < 50; $i++) {
                $arr1 = [];
                $arr2 = [];
                for ($j = 0; $j < 10; $j++) {
                    $item1 = [
                        'type' => 'add',
                        'name' => StringHelper::randString(4, 5),
                        'age'  => rand(1, 99),
                    ];
                    $item2 = [
                        'type'   => 'add',
                        'order'  => StringHelper::randSimple(32),
                        'status' => boolval(mt_rand(0, 1)),
                    ];
                    array_push($arr1, $item1);
                    array_push($arr2, $item2);
                }

                $queue1->addMulti(...$arr1);
                $queue2->addMulti(...$arr2);
            }

            $len1 = $queue1->len();
            $len2 = $queue2->len();
            $this->assertEquals(500, $len1);
            $this->assertEquals(500, $len2);
        } catch (QueueException $e) {
        }
    }


    public function testPush() {
        try {
            $client = RedisConn::getRedis(ConnTest::$conf);
            $queue1 = RedisQueue::setDefaultRedis($client)->newQueue(self::$que1cnf);
            $queue2 = RedisQueue::setDefaultRedis($client)->newQueue(self::$que2cnf);

            for ($i = 0; $i < 500; $i++) {
                $item1 = [
                    'type' => 'push',
                    'name' => StringHelper::randString(4, 5),
                    'age'  => rand(1, 99),
                ];
                $item2 = [
                    'type'   => 'push',
                    'order'  => StringHelper::randSimple(32),
                    'status' => boolval(mt_rand(0, 1)),
                ];

                $queue1->push($item1, mt_rand(1, 99));
                $queue2->push($item2, mt_rand(1, 99));
            }

            $len1 = $queue1->len();
            $len2 = $queue2->len();
            $this->assertEquals(1000, $len1);
            $this->assertEquals(1000, $len2);
        } catch (QueueException $e) {
        }
    }


    public function testPushMulti() {
        try {
            $client = RedisConn::getRedis(ConnTest::$conf);
            $queue1 = RedisQueue::setDefaultRedis($client)->newQueue(self::$que1cnf);
            $queue2 = RedisQueue::setDefaultRedis($client)->newQueue(self::$que2cnf);

            for ($i = 0; $i < 50; $i++) {
                $arr1 = [];
                $arr2 = [];
                for ($j = 0; $j < 10; $j++) {
                    $item1 = [
                        'type' => 'push',
                        'name' => StringHelper::randString(4, 5),
                        'age'  => rand(1, 99),
                    ];
                    $item2 = [
                        'type'   => 'push',
                        'order'  => StringHelper::randSimple(32),
                        'status' => boolval(mt_rand(0, 1)),
                    ];
                    array_push($arr1, $item1);
                    array_push($arr2, $item2);
                }

                $queue1->pushMulti(...$arr1);
                $queue2->pushMulti(...$arr2);
            }

            $len1 = $queue1->len();
            $len2 = $queue2->len();
            $this->assertEquals(1500, $len1);
            $this->assertEquals(1500, $len2);
        } catch (QueueException $e) {
        }
    }


    public function testShiftPop() {
        try {
            $client = RedisConn::getRedis(ConnTest::$conf);
            $queue1 = RedisQueue::setDefaultRedis($client)->newQueue(self::$que1cnf);
            $queue2 = RedisQueue::setDefaultRedis($client)->newQueue(self::$que2cnf);

            $itm1 = $queue1->shift();
            $itm2 = $queue2->shift();

            $itm3 = $queue1->pop();
            $itm4 = $queue2->pop();

            $this->assertNotEmpty($itm1);
            $this->assertNotEmpty($itm2);
            $this->assertNotEmpty($itm3);
            $this->assertNotEmpty($itm4);
        } catch (QueueException $e) {
        }
    }


}