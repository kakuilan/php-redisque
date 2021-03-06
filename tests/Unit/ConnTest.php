<?php
/**
 * Copyright (c) 2021 LKK All rights reserved
 * User: kakuilan
 * Date: 2021/1/21
 * Time: 18:14
 * Desc:
 */

namespace Redisque\Tests\Unit;

use PHPUnit\Framework\TestCase;
use Error;
use Exception;
use Throwable;
use Redisque\RedisConn;
use Redisque\RedisClient;


/**
 * Class ConnTest
 * @package Redisque\Tests\Unit
 */
class ConnTest extends TestCase {


    /**
     * @var array
     */
    public static $conf = [
        'host'     => 'localhost',
        'password' => '',
//        'host'     => '192.168.56.1',
//        'password' => '123456',
        'port'     => 6379,
        'select'   => 0,
    ];


    public function testDefaultConf() {
        RedisConn::setDefaultConf(self::$conf);

        $conf = RedisConn::getDefaultConf();
        $this->assertEquals($conf['password'], self::$conf['password']);
    }


    public function testGetRedis() {
        try {
            $client = RedisConn::getRedis(self::$conf);
            $info   = $client->info();
            $this->assertTrue(is_array($info));
            $this->assertTrue($client instanceof RedisClient);

            /* @var $client2 RedisClient */
            $client2 = RedisConn::getRedis(self::$conf);
            $time1   = $client->getLastConnectTime();
            $time2   = $client2->getLastConnectTime();
            $this->assertEquals($time1, $time2);

            /* @var $client3 RedisClient */
            $client3 = RedisConn::getRedis([]);
            $time3   = $client3->getLastConnectTime();
            $this->assertEquals($time3, $time2);

            $conf    = array_merge(self::$conf, ['wait_timeout' => 1]);
            $client4 = RedisConn::getRedis($conf);
            sleep(2);
            $client5 = RedisConn::getRedis($conf);

            $conf2   = array_merge(self::$conf, ['password' => '654321']);
            $client6 = RedisConn::getRedis($conf2);

            $client7 = RedisConn::getRedis($conf2);
        } catch (Throwable $e) {
            //var_dump($e->getMessage());
        }
    }


}