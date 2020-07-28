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
use RedisException;
use Throwable;
use Error;

/**
 * Class RedisQueue
 * @package Redisque
 */
class RedisQueue extends BaseService {


    /**
     * 队列相关键前缀
     * @var string
     */
    protected $prefix = 'que_';


    /**
     * redis配置
     * @var array
     */
    protected $conf = [];


    /**
     * 默认配置
     * @var array
     */
    public static $defaultConf = [
        'host'         => '127.0.0.1',
        'port'         => 6379,
        'password'     => null,
        'database'     => 0,
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


    /**
     * 根据配置获取redis客户端
     * @param array $conf
     * @return Redis
     * @throws QueueException
     */
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

            if (!$ret) {
                throw new QueueException(QueueException::ERR_CANNOT_CONNECT_MSG, QueueException::ERR_CANNOT_CONNECT_CODE);
            }

            $redis->select($conf['database']);

            $connInfo = [
                'redis'             => $redis,
                'last_connect_time' => $now,
            ];

            self::$clients[$clientKey] = $connInfo;
        }

        return $connInfo['redis'];
    }


    /**
     * 根据键获取redis客户端
     * @param string $key 客户端标识键
     * @return Redis
     * @throws QueueException
     */
    public static function getRedisByKey(string $key = ''): Redis {
        if (empty($key) || !isset(self::$clients[$key])) {
            throw new QueueException(QueueException::ERR_CLIENT_NOTEXIST_MSG, QueueException::ERR_CLIENT_NOTEXIST_CODE);
        }

        $connInfo      = self::$clients[$key];
        $now           = time();
        $socketTimeout = ini_get('default_socket_timeout');
        $waitTimeout   = intval(self::$defaultConf['wait_timeout']);
        if ($socketTimeout > 0) {
            $waitTimeout = min($socketTimeout, $waitTimeout);
        }

        $lastTime   = $connInfo['last_connect_time'] ?? 0; //上次连接时间
        $expireTime = $lastTime + $waitTimeout; //预计过期时间

        if (!($now >= $lastTime && $now < $expireTime)) {
            $pingRes = false;
            try {
                $ping    = $connInfo['redis']->ping();
                $pingRes = (strpos($ping, "PONG") !== false);
            } catch (Error $e) {
            }

            if (!$pingRes) {
                $msg = QueueException::ERR_CANNOT_CONNECT_MSG . $e->getMessage();
                throw new QueueException($msg, QueueException::ERR_CANNOT_CONNECT_CODE);
            }

            self::$clients[$key]['last_connect_time'] = $now;
        }

        return $connInfo['redis'];
    }


    /**
     * 设置redis客户端对象
     * @param string $clientKey 客户端标识键
     * @param Redis $redis 客户端对象
     * @return bool
     */
    public function setRedis(string $clientKey, Redis $redis): bool {
        try {
            $ping = $redis->ping();
            $res  = strpos($ping, "PONG") !== false;
        } catch (Throwable $e) {
            $res = false;
        }

        if ($res) {
            $connInfo = [
                'redis'             => $redis,
                'last_connect_time' => time(),
            ];

            self::$clients[$clientKey] = $connInfo;
        }

        return $res;
    }


    public function getRedis($param = null): Redis {

    }


    /**
     * 设置前缀
     * @param string $str
     */
    public function setPrefix(string $str) {
        $this->prefix = $str;
    }


}