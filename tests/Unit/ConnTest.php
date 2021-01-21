<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2021/1/21
 * Time: 18:14
 * Desc:
 */

namespace Redisque\Tests\Unit;

use PHPUnit\Framework\TestCase;
use Error;
use Exception;
use Redisque\RedisConn;


/**
 * Class ConnTest
 * @package Redisque\Tests\Unit
 */
class ConnTest extends TestCase {


    /**
     * @var array
     */
    public static $conf = [
        'host'     => '192.168.56.1',
        'port'     => 6379,
        'password' => '123456',
        'select'   => 1,
    ];


    public function testDefaultConf() {
        RedisConn::setDefaultConf(self::$conf);

        $conf = RedisConn::getDefaultConf();
        $this->assertEquals($conf['password'], self::$conf['password']);
    }


}