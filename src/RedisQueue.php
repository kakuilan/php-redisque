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
use Kph\Helpers\ArrayHelper;
use Kph\Helpers\ValidateHelper;
use Redis;
use Throwable;
use Error;

/**
 * Class RedisQueue
 * @package Redisque
 */
class RedisQueue extends BaseService {


    /**
     * 默认配置
     * @var array
     */
    public static $defaultConf = [
        'host'         => '127.0.0.1',
        'port'         => 6379,
        'password'     => null,
        'database'     => 0,
        'prefix'       => 'que_', //key前缀
        'wait_timeout' => Consts::TTL_TWO_MINUTE,
    ];


    /**
     * redis持久ID
     * @var string
     */
    protected static $persistentId = 'queue_conn_';


    /**
     * redis客户端对象缓存数组
     * @var array
     */
    public static $clients = [];


    public static function getRedisByConf(array $conf = []): Redis {
        if (empty($conf)) {
            $conf = self::$defaultConf;
        }

        //检查配置的字段,必须和默认配置的相同
        if (!ValidateHelper::isEqualArray(self::$defaultConf, $conf)) {
            $msg = QueueException::ERR_CONF_MSG . implode(',', array_keys(self::$defaultConf)) . '.';
            throw new QueueException($msg, QueueException::ERR_CONF_CODE);
        }

        sort($conf);
        $clientKey = md5(json_encode($conf));

        //redis客户端连接信息
        $connInfo      = self::$clients[$clientKey] ?? null;
        $now           = time();
        $socketTimeout = ini_get('default_socket_timeout');
        $waitTimeout   = intval($conf['wait_timeout']);
        if ($socketTimeout > 0) {
            $waitTimeout = min($socketTimeout, $waitTimeout);
        }

        $lastTime   = $connInfo['last_connect_time'] ?? 0; //上次连接时间
        $expireTime = $lastTime + $waitTimeout; //预计过期时间

        $pingRes = false;
        if ($connInfo) {
            if (!($now >= $lastTime && $now < $expireTime)) {
                try {
                    $ping    = $connInfo['redis']->ping();
                    $pingRes = (strpos($ping, "PONG") !== false);
                } catch (Error $e) {
                }
            }
        }

        //新建连接
        if (empty($connInfo) || !$pingRes) {
            $redis        = new Redis();
            $persistentId = self::$persistentId . $clientKey;
            $ret          = $redis->pconnect($conf['host'], $conf['port'], $waitTimeout + 1, $persistentId);
            if (!empty($conf['password'])) {
                $ret = $redis->auth($conf['password']);
            }

            if(!$ret) {
                $msg = QueueException::ERR_CONF_MSG . implode(',', array_keys(self::$defaultConf)) . '.';
                throw new QueueException($msg, QueueException::ERR_CONF_CODE);
            }

            $redis->select($conf['database']);

            $connInfo = [
                'redis'             => $redis,
                'last_connect_time' => $now,
            ];

            self::$clients[$clientKey] = $connInfo;
        }

    }


    public static function getRedisByKey(string $key = ''): Redis {

    }


    public function setRedis(string $clientKey, Redis $redis): bool {

    }


    public function getRedis($param = null): Redis {

    }


}