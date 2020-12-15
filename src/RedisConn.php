<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2020/12/15
 * Time: 10:23
 * Desc:
 */

namespace Redisque;

use Kph\Helpers\ArrayHelper;
use Kph\Services\BaseService;
use Redis;
use RedisException;
use Exception;
use Error;
use Throwable;

/**
 * Class RedisClient
 * @package Redisque
 */
class RedisConn extends BaseService {


    /**
     * redis连接数组
     * @var array
     */
    protected static $conns = [];


    /**
     * 默认配置
     * @var array
     */
    protected static $defaultConf = [
        'host'         => '127.0.0.1',
        'port'         => 6379,
        'password'     => null,
        'select'       => null,
        'wait_timeout' => 120, //保持连接超时,秒
    ];


    /**
     * 设置默认配置
     * @param array $conf
     */
    public static function setDefaultConf(array $conf): void {
        if (!empty($conf)) {
            self::$defaultConf = array_merge(self::$defaultConf, $conf);
        }
    }


    /**
     * 获取redis客户端
     * @param array $conf redis配置
     * @return array
     */
    public static function getRedis(array $conf): array {
        if (empty($conf)) {
            $conf = self::$defaultConf;
        }
        ArrayHelper::regularSort($conf);

        $key           = md5(json_encode($conf));
        $now           = time();
        $conn          = self::$conns[$key] ?? [];
        $socketTimeout = ini_get('default_socket_timeout');
        $waitTimeout   = intval($conf['wait_timeout'] ?? 120);
        $lastTime      = $conn['last_connect_time'] ?? 0;
        $persistentId  = $key; //长连接ID

        if ($socketTimeout > 0) {
            $waitTimeout = min($socketTimeout, $waitTimeout);
        }

        $maxTime = $lastTime + $waitTimeout;
        $pingRes = false;
        if ($conn) {
            $pingRes = true;
            if (!($now >= $lastTime && $now < $maxTime)) {
                try {
                    $ping    = $conn['redis']->ping();
                    $pingRes = (strpos($ping, "PONG") !== false);
                } catch (Throwable $e) {
                    $pingRes = false;
                }
            }
        }

        if (empty($conn) || !$pingRes) {
            $redis   = new Redis();
            $pingRes = $redis->pconnect($conf['host'], $conf['port'], 0, $persistentId);
            if (isset($conf['password']) && !empty($conf['password'])) {
                $pingRes = $redis->auth($conf['password']);
            }

            $selectDb = (isset($conf['select']) && is_int($conf['select'])) ? $conf['select'] : 0;
            $redis->setOption(Redis::OPT_SERIALIZER, Redis::SERIALIZER_PHP);
            $redis->select($selectDb);

            $conn = [
                'last_connect_time' => $now,
                'redis'             => $redis,
            ];

            self::$conns[$key] = $conn;
        }

        return $pingRes ? $conn : [];
    }


}