<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2020/7/18
 * Time: 20:04
 * Desc: redis队列
 */

namespace Redisque;

use Kph\Services\BaseService;
use Kph\Consts;
use Redis;

class RedisQueue extends BaseService {


    /**
     * 默认配置
     * @var array
     */
    public static $defaultConf = [
        'host'         => '127.0.0.1',
        'port'         => 6379,
        'password'     => null,
        'select'       => 0,
        'wait_timeout' => Consts::TTL_TWO_MINUTE,
    ];





}