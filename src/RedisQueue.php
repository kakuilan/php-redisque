<?php
/**
 * Copyright (c) 2020 LKK All rights reserved
 * User: kakuilan
 * Date: 2020/7/18
 * Time: 20:04
 * Desc: Redis队列
 */

namespace Redisque;

use Kph\Exceptions\BaseException;
use Kph\Services\BaseService;
use Kph\Consts;
use Kph\Helpers\ArrayHelper;
use Kph\Helpers\StringHelper;
use Kph\Helpers\DateHelper;
use Kph\Helpers\ValidateHelper;
use Redis;
use RedisException;
use Exception;
use Error;
use Throwable;

/**
 * Class RedisQueue
 * @package Redisque
 */
class RedisQueue extends BaseService implements QueueInterface {

    /**
     * 队列类型-有序队列(有序集合)
     */
    const QUEUE_TYPE_ISSORT = 'issort';


    /**
     * 队列类型-无序队列(列表),先进先出
     */
    const QUEUE_TYPE_NOSORT = 'nosort';


    /**
     * 非优先队列-无及时性要求,如日志
     */
    const QUEUE_PRIORITY_NO = 0;


    /**
     * 优先队列-对及时性有要求,如短信
     */
    const QUEUE_PRIORITY_IS = 1;


    /**
     * 中转队列名
     */
    const QUEUE_TRANS_NAME = [
        0 => 'transfer_que_common', //非优先队列中转
        1 => 'transfer_que_important', //优先队列中转
    ];


    /**
     * 中转哈希表
     */
    const QUEUE_TRANS_TABL = [
        0 => 'transfer_tab_common', //非优先队列中转
        1 => 'transfer_tab_important', //优先队列中转
    ];


    /**
     * 中转队列的锁名
     */
    const QUEUE_TRANS_LOCK_KEY = [
        0 => 'transfer_lock_common', //非优先队列中转
        1 => 'transfer_lock_important', //优先队列中转
    ];


    /**
     * 中转队列的锁时间,秒
     */
    const QUEUE_TRANS_LOCK_TIME = [
        0 => Consts::TTL_ONE_HOUR, //非优先队列中转
        1 => Consts::TTL_FIF_MINUTE, //优先队列中转
    ];


    /**
     * 所有队列名称的哈希表 ['队列名'=>'队列的键']
     */
    const QUEUE_ALL_NAME = 'all_queues';


    /**
     * 包装消息的消息字段名
     */
    const WRAP_ITEM_FIELD = 'queMsgItem';


    /**
     * 包装消息的权重字段名
     */
    const WRAP_WEIGHT_FIELD = 'queMsgWeight';


    /**
     * 包装消息的有效期字段名
     */
    const WRAP_EXPIRE_FIELD = 'queMsgExpire';


    /**
     * 队列名
     * @var string
     */
    protected $queueName = '';


    /**
     * 是否有序队列
     * @var bool
     */
    protected $isSort = false;


    /**
     * 是否优先队列
     * @var int
     */
    protected $priority = 0;


    /**
     * 消息有效期,秒,0永久
     * @var int
     */
    protected $expire = 0;


    /**
     * 中转队列重新入栈时间,秒
     * @var int
     */
    protected $transTime = 0;


    /**
     * redis连接名
     * @var string
     */
    protected $connName = '';


    /**
     * 默认的redis客户端
     * @var null|Redis
     */
    protected static $redis = null;


    /**
     * 队列名缓存
     * @var array
     */
    protected static $queueNamesCache = [];


    /**
     * 获取key的前缀
     * @return string
     */
    public static function getPrefix(): string {
        $res = self::getClass() . ':';
        return str_replace('\\', '-', $res);
    }

    /**
     * 设置默认的redis客户端连接
     * @param Redis $client 客户端
     * @return static
     */
    public static function setDefaultRedis(Redis $client): QueueInterface {
        self::$redis = $client;
        return new static();
    }

    /**
     * 获取默认的redis客户端连接
     * @return Redis
     * @throws Throwable
     */
    public static function getDefaultRedis(): Redis {
        if (is_null(self::$redis)) {
            return RedisConn::getRedis([]);
        }

        return self::$redis;
    }

    /**
     * 获取所有队列名
     * @param bool $readCache 是否从缓存读取
     * @param null|mixed $redis Redis客户端对象
     * @return array
     */
    public static function getQueues(bool $readCache = true, $redis = null): array {
        $arr = self::$queueNamesCache;
        if (empty($arr) || !$readCache) {
            if (empty($redis)) {
                try {
                    $redis = self::getDefaultRedis();
                } catch (Throwable $e) {
                }
            }
            if (!is_object($redis) || !($redis instanceof Redis)) {
                return [];
            }

            $key = self::getTableKey();
            $arr = (array)$redis->hGetAll($key);
            if (!empty($arr)) {
                self::$queueNamesCache = $arr;
            }
        }

        return $arr ? array_keys($arr) : [];
    }

    /**
     * 统计队列数
     * @param bool $readCache 是否从缓存读取
     * @param null|mixed $redis Redis客户端对象
     * @return int
     */
    public static function countQueues(bool $readCache = true, $redis = null): int {
        $arr = self::getQueues($readCache, $redis);

        return $arr ? count($arr) : 0;
    }

    /**
     * 检查队列是否存在
     * @param string $queueName 队列名
     * @param null|mixed $redis Redis客户端对象
     * @return bool
     */
    public static function queueExists(string $queueName, $redis = null): bool {
        $res = $queueName != '' && isset(self::$queueNamesCache[$queueName]);
        if ($queueName && !$res) {
            if (empty($redis)) {
                try {
                    $redis = self::getDefaultRedis();
                } catch (Throwable $e) {
                }
            }
            if (!is_object($redis) || !($redis instanceof Redis)) {
                return false;
            }

            $key      = self::getTableKey();
            $queueKey = $redis->hGet($key, $queueName);
            $res      = !empty($queueKey);
            if ($res) {
                self::$queueNamesCache[$queueName] = $queueKey;
            }
        }

        return $res;
    }

    /**
     * 获取哈希表(存储全部队列名)的键名
     * @return string
     */
    public static function getTableKey(): string {
        return self::getPrefix() . self::QUEUE_ALL_NAME;
    }

    /**
     * 获取(消费)队列的键名
     * @param string $queueName 队列名
     * @param null|mixed $redis Redis客户端对象
     * @return string
     */
    public static function getQueueKey(string $queueName, $redis = null): string {
        $res = self::$queueNamesCache[$queueName] ?? '';
        if ($res == '') {
            if (empty($redis)) {
                try {
                    $redis = self::getDefaultRedis();
                } catch (Throwable $e) {
                }
            }
            if (!is_object($redis) || !($redis instanceof Redis)) {
                return '';
            }

            $key = self::getTableKey();
            $res = $redis->hGet($key, $queueName);
        }

        return strval($res);
    }

    /**
     * 获取中转哈希表(存储中转消息)的键名
     * @param int $priority 是否优先
     * @return string
     */
    public static function getTransTableKey(int $priority): string {
        if (!in_array($priority, [self::QUEUE_PRIORITY_NO, self::QUEUE_PRIORITY_IS])) {
            $priority = self::QUEUE_PRIORITY_NO;
        }

        return self::getPrefix() . self::QUEUE_TRANS_TABL[$priority];
    }

    /**
     * 获取中转队列(存储中转消息key)的键名
     * @param int $priority
     * @return string
     */
    public static function getTransQueueKey(int $priority): string {
        if (!in_array($priority, [self::QUEUE_PRIORITY_NO, self::QUEUE_PRIORITY_IS])) {
            $priority = self::QUEUE_PRIORITY_NO;
        }

        return self::getPrefix() . self::QUEUE_TRANS_NAME[$priority];
    }

    /**
     * 获取操作key
     * @param string $operation 操作名
     * @param mixed $dataId 数据ID
     * @return string
     */
    protected static function getOperateKey(string $operation, $dataId): string {
        $dataId = strval($dataId);
        return self::getPrefix() . "operate_lock:{$operation}:{$dataId}";
    }

    /**
     * 获取操作锁
     * @param string $operation 操作名
     * @param mixed $dataId 数据ID
     * @param int $operateUid 当前操作者UID
     * @param int $ttl 有效期
     * @param null|mixed $redis Redis客户端对象
     * @return int 获取到锁的UID:>0时为本身;<=0时为他人
     */
    public static function getLockOperate(string $operation, $dataId, int $operateUid, int $ttl = 60, $redis = null): int {
        $res    = 0;
        $dataId = strval($dataId);
        if ($operation == '' || $dataId == '' || $operateUid <= 0) {
            return $res;
        }

        if (empty($redis)) {
            try {
                $redis = self::getDefaultRedis();
            } catch (Throwable $e) {
            }
        }
        if (!is_object($redis) || !($redis instanceof Redis)) {
            return $res;
        }

        if ($ttl <= 0) {
            $ttl = 60;
        }

        $now    = time();
        $expire = $now + $ttl;
        $key    = self::getOperateKey($operation, $dataId);
        $data   = implode('|', [$operateUid, $expire]);

        if ($ret = $redis->setnx($key, $data)) {
            $redis->expire($key, $ttl);
            $res = $operateUid;
        } else {
            $val = $redis->get($key);
            $arr = $val ? explode('|', $val) : [];
            $uid = $arr[0] ?? 0;
            $exp = $arr[1] ?? 0;
            if (empty($val) || $uid == 0) {
                if ($ret = $redis->setnx($key, $data)) {
                    $redis->expire($key, $ttl);
                    $res = $operateUid;
                }
            } else {
                if ($uid == $operateUid || ($now > $exp)) {
                    if ($ret = $redis->setnx($key, $data)) {
                        $redis->expire($key, $ttl);
                        $res = $operateUid;
                    }
                } else {
                    $res = -abs($uid);
                }
            }
        }

        return $res;
    }

    /**
     * 解锁操作
     * @param string $operation 操作名
     * @param mixed $dataId 数据ID
     * @param null|mixed $redis Redis客户端对象
     * @return bool
     */
    public static function unlockOperate(string $operation, $dataId, $redis = null): bool {
        $res    = false;
        $dataId = strval($dataId);
        if ($operation == '' || $dataId == '') {
            return $res;
        }

        if (empty($redis)) {
            try {
                $redis = self::getDefaultRedis();
            } catch (Throwable $e) {
            }
        }
        if (!is_object($redis) || !($redis instanceof Redis)) {
            return $res;
        }

        $key = self::getOperateKey($operation, $dataId);
        $redis->del($key);

        return true;
    }

    /**
     * 新建队列/设置队列
     * @param array $conf 队列配置
     * @return static
     * @throws Throwable
     */
    public function newQueue(array $conf): QueueInterface {
        $queueName = StringHelper::trim($conf['queueName'] ?? ''); //队列名
        $connName  = StringHelper::trim($conf['connName'] ?? ''); //Redis连接名
        $isSort    = boolval($conf['isSort'] ?? false); //是否有序队列
        $priority  = intval($conf['priority'] ?? 0); //是否优先队列
        $expire    = intval($conf['expire'] ?? 0); //消息有效期
        $transTime = intval($conf['transTime'] ?? 0); //处理中转队列的时间

        $priority = $priority ? self::QUEUE_PRIORITY_IS : self::QUEUE_PRIORITY_NO;
        $sortType = $isSort ? self::QUEUE_TYPE_ISSORT : self::QUEUE_TYPE_NOSORT;

        if ($expire < 0) {
            $expire = Consts::TTL_DEFAULT;
        }
        if ($transTime <= 0) {
            $transTime = Consts::TTL_DEFAULT;
        }

        if ($queueName == '') {
            throw new BaseException(QueueException::ERR_MESG_QUEUE_NAMEEMPTY, QueueException::ERR_CODE_QUEUE_NAMEEMPTY);
        }

        /* @var $queInfo QueueInfo */
        $queInfo = $this->getQueueInfo($queueName);
        if (ValidateHelper::isEmptyObject($queInfo)) {
            $ret = $this->addQueueName($queueName, $sortType, $priority);
            if (!$ret) {
                throw new QueueException($this->getError(), $this->getErrno());
            }
        } elseif ($queInfo->isSort !== $isSort || $queInfo->priority !== $priority) {
            throw new BaseException(QueueException::ERR_MESG_QUEUE_EXIST_TYPECONFLICT, QueueException::ERR_CODE_QUEUE_EXIST_TYPECONFLICT);
        }

        $this->queueName = $queueName;
        $this->connName  = $connName;
        $this->isSort    = $isSort;
        $this->priority  = $priority;
        $this->expire    = $expire;
        $this->transTime = $transTime;

        return $this;
    }

    /**
     * 获取redis客户端连接
     * @param string $connName 连接名
     * @return Redis
     * @throws Throwable
     */
    public function getRedisClient(string $connName = ''): Redis {
        if ($connName == '') {
            $connName = $this->connName;
        }

        return self::getDefaultRedis();
    }

    /**
     * 获取队列信息
     * @param string $queueName 队列名
     * @return object
     */
    public function getQueueInfo(string $queueName = ''): object {
        $res = [];
        if ($queueName == '') {
            $queueName = $this->queueName;
        }
        if (empty($queueName)) {
            $this->setErrorInfo(QueueException::ERR_MESG_QUEUE_NAMEEMPTY, QueueException::ERR_CODE_QUEUE_NAMEEMPTY);
            return (object)$res;
        }

        try {
            $client   = $this->getRedisClient($this->connName);
            $queueKey = self::getQueueKey($queueName, $client);
            $arr      = explode(Consts::DELIMITER, $queueKey);
            if (!empty($arr) && is_array($arr)) {
                [, $queueName, $sortType, $priority, $expire] = $arr;
                $res            = new QueueInfo();
                $res->queueName = $queueName;
                $res->queueKey  = $queueKey;
                $res->isSort    = ($sortType == self::QUEUE_TYPE_NOSORT ? false : true);
                $res->priority  = intval($priority);
                $res->expire    = intval($expire);
            } else {
                $this->setErrorInfo(QueueException::ERR_MESG_QUEUE_INFO_FAIL, QueueException::ERR_CODE_QUEUE_INFO_FAIL);
            }
        } catch (Throwable $e) {
            $this->setErrorInfo(QueueException::ERR_MESG_CLIENT_CANNOT_CONNECT, QueueException::ERR_CODE_CLIENT_CANNOT_CONNECT);
        }

        return (object)$res;
    }

    /**
     * 添加队列名(到哈希表)
     * @param string $queueName 队列名
     * @param string $sortType 排序类型
     * @param int $priority 是否优先
     * @return bool
     */
    public function addQueueName(string $queueName, string $sortType, int $priority): bool {
        $res = false;
        if ($queueName == '') {
            $this->setErrorInfo(QueueException::ERR_MESG_QUEUE_NAMEEMPTY, QueueException::ERR_CODE_QUEUE_NAMEEMPTY);
            return $res;
        }

        $arr = [
            self::getPrefix(),
            $queueName,
            $sortType,
            $priority,
        ];

        $queueKey = implode(Consts::DELIMITER, $arr);
        $key      = self::getTableKey();
        try {
            $client = $this->getRedisClient($this->connName);
            $res    = $client->hSetNx($key, $queueName, $queueKey);
            if ($res) {
                self::$queueNamesCache[$queueName] = $queueKey;
            }
        } catch (Throwable $e) {
            $this->setErrorInfo(QueueException::ERR_MESG_CLIENT_CANNOT_CONNECT, QueueException::ERR_CODE_CLIENT_CANNOT_CONNECT);
        }

        return boolval($res);
    }

    /**
     * 消息是否经过包装
     * @param array $msg
     * @return bool
     */
    public static function isWraped(array $msg): bool {
        if (!isset($msg[self::WRAP_ITEM_FIELD]) || !isset($msg[self::WRAP_WEIGHT_FIELD]) || !isset($msg[self::WRAP_EXPIRE_FIELD])) {
            return false;
        }

        $fields = [self::WRAP_ITEM_FIELD, self::WRAP_WEIGHT_FIELD, self::WRAP_EXPIRE_FIELD];
        $keys   = array_keys($msg);

        return ValidateHelper::isEqualArray($fields, $keys);
    }

    /**
     * (有序队列的)消息包装
     * @param array $msg 原始消息
     * @param int $weight 权重,0~99,值越大在队列中越排前,仅对有序队列起作用
     * @param int $expire 消息有效期(秒);默认0,为永久
     * @return array
     */
    public static function wrapMsg(array $msg, int $weight = 0, int $expire = 0): array {
        if (self::isWraped($msg)) {
            return $msg;
        }

        $time   = microtime(true);
        $sub    = substr($time, 2, 12);
        $weight = ($weight < 0) ? 0 : min(99, $weight);
        $score  = 100 - $weight;
        $score  = $score * pow(10, 8) + $sub;

        if ($expire > 0) {
            $expire += time();
        } else {
            $expire = 0;
        }

        return [
            self::WRAP_ITEM_FIELD   => $msg,
            self::WRAP_WEIGHT_FIELD => $score,
            self::WRAP_EXPIRE_FIELD => $expire,
        ];
    }

    /**
     * 消息反包装
     * @param array $msg 经包装的消息unwrapMsg
     * @return array
     */
    public static function unwrapMsg(array $msg): array {
        if (!self::isWraped($msg)) {
            return $msg;
        }

        return $msg[self::WRAP_ITEM_FIELD];
    }

    /**
     * 消息打包
     * @param array $msg
     * @return string
     */
    public function pack(array $msg): string {
        if (!self::isWraped($msg)) {
            $msg = self::wrapMsg($msg, 0, $this->expire);
        }

        return json_encode($msg);
    }


    /**
     * 消息解包
     * @param string $msg
     * @return array
     */
    public function unpack(string $msg): array {
        if (empty($msg) || !ValidateHelper::isJson($msg)) {
            return [];
        }

        return self::unwrapMsg(json_decode($msg, true));
    }

    /**
     * 队列头压入一个消息
     * @param array $msg
     * @param int $weight 权重,0~99,值越大在队列中越排前,仅对有序队列起作用
     * @return bool
     */
    public function add(array $msg, int $weight = 0): bool {
        $res = false;
        if (empty($msg)) {
            $this->setErrorInfo(QueueException::ERR_MESG_QUEUE_MESSAG_EEMPTY, QueueException::ERR_CODE_QUEUE_MESSAG_EEMPTY);
            return $res;
        }

        /* @var $queInfo QueueInfo */
        $queInfo = $this->getQueueInfo();
        if (ValidateHelper::isEmptyObject($queInfo)) {
            return $res;
        }

        try {
            $client = $this->getRedisClient($this->connName);
            $msg    = self::wrapMsg($msg, $weight, $this->expire);
            if ($queInfo->isSort) {
                $ret = $client->zAdd($queInfo->queueKey, $msg[self::WRAP_WEIGHT_FIELD], $this->pack($msg));
            } else {
                $ret = $client->lPush($queInfo->queueKey, $this->pack($msg));
            }
            if ($ret) {
                $res = true;
            } else {
                $this->setErrorInfo(QueueException::ERR_MESG_QUEUE_OPERATE_FAIL, QueueException::ERR_CODE_QUEUE_OPERATE_FAIL);
            }
        } catch (Throwable $e) {
            $this->setErrorInfo(QueueException::ERR_MESG_CLIENT_CANNOT_CONNECT, QueueException::ERR_CODE_CLIENT_CANNOT_CONNECT);
        }

        return $res;
    }

    /**
     * 队列头压入多个个消息
     * @param array ...$msgs
     * @return bool
     */
    public function addMulti(array ...$msgs): bool {
        $res = false;
        if (empty($msgs)) {
            $this->setErrorInfo(QueueException::ERR_MESG_QUEUE_MESSAG_EEMPTY, QueueException::ERR_CODE_QUEUE_MESSAG_EEMPTY);
            return $res;
        }

        /* @var $queInfo QueueInfo */
        $queInfo = $this->getQueueInfo();
        if (ValidateHelper::isEmptyObject($queInfo)) {
            return $res;
        }

        try {
            $client = $this->getRedisClient($this->connName);

            $client->multi();
            foreach ($msgs as $msg) {
                $msg = self::wrapMsg($msg, 0, $this->expire);
                if ($queInfo->isSort) {
                    $client->zAdd($queInfo->queueKey, $msg[self::WRAP_WEIGHT_FIELD], $this->pack($msg));
                } else {
                    $client->lPush($queInfo->queueKey, $this->pack($msg));
                }
            }
            $mulRes = $client->exec();
            if (is_array($mulRes) && isset($mulRes[0]) && !empty($mulRes[0])) {
                $res = true;
            } else {
                $this->setErrorInfo(QueueException::ERR_MESG_QUEUE_OPERATE_FAIL, QueueException::ERR_CODE_QUEUE_OPERATE_FAIL);
            }
        } catch (Throwable $e) {
            $this->setErrorInfo(QueueException::ERR_MESG_CLIENT_CANNOT_CONNECT, QueueException::ERR_CODE_CLIENT_CANNOT_CONNECT);
        }

        return $res;
    }

    /**
     * 队列尾压入一个消息
     * @param array $msg
     * @param int $weight 权重,0~99,值越大在队列中越排前,仅对有序队列起作用
     * @return bool
     */
    public function push(array $msg, int $weight = 0): bool {
        $res = false;
        if (empty($msg)) {
            $this->setErrorInfo(QueueException::ERR_MESG_QUEUE_MESSAG_EEMPTY, QueueException::ERR_CODE_QUEUE_MESSAG_EEMPTY);
            return $res;
        }

        /* @var $queInfo QueueInfo */
        $queInfo = $this->getQueueInfo();
        if (ValidateHelper::isEmptyObject($queInfo)) {
            return $res;
        }

        try {
            $client = $this->getRedisClient($this->connName);
            $msg    = self::wrapMsg($msg, $weight, $this->expire);
            if ($queInfo->isSort) {
                $ret = $client->zAdd($queInfo->queueKey, $msg[self::WRAP_WEIGHT_FIELD], $this->pack($msg));
            } else {
                $ret = $client->rPush($queInfo->queueKey, $this->pack($msg));
            }
            if ($ret) {
                $res = true;
            } else {
                $this->setErrorInfo(QueueException::ERR_MESG_QUEUE_OPERATE_FAIL, QueueException::ERR_CODE_QUEUE_OPERATE_FAIL);
            }
        } catch (Throwable $e) {
            $this->setErrorInfo(QueueException::ERR_MESG_CLIENT_CANNOT_CONNECT, QueueException::ERR_CODE_CLIENT_CANNOT_CONNECT);
        }

        return $res;
    }

    /**
     * 队列尾压入多个消息
     * @param array ...$msgs
     * @return bool
     */
    public function pushMulti(array ...$msgs): bool {
        $res = false;
        if (empty($msgs)) {
            $this->setErrorInfo(QueueException::ERR_MESG_QUEUE_MESSAG_EEMPTY, QueueException::ERR_CODE_QUEUE_MESSAG_EEMPTY);
            return $res;
        }

        /* @var $queInfo QueueInfo */
        $queInfo = $this->getQueueInfo();
        if (ValidateHelper::isEmptyObject($queInfo)) {
            return $res;
        }

        try {
            $client = $this->getRedisClient($this->connName);

            $client->multi();
            foreach ($msgs as $msg) {
                $msg = self::wrapMsg($msg, 0, $this->expire);
                if ($queInfo->isSort) {
                    $client->zAdd($queInfo->queueKey, $msg[self::WRAP_WEIGHT_FIELD], $this->pack($msg));
                } else {
                    $client->rPush($queInfo->queueKey, $this->pack($msg));
                }
            }
            $mulRes = $client->exec();
            if (is_array($mulRes) && isset($mulRes[0]) && !empty($mulRes[0])) {
                $res = true;
            } else {
                $this->setErrorInfo(QueueException::ERR_MESG_QUEUE_OPERATE_FAIL, QueueException::ERR_CODE_QUEUE_OPERATE_FAIL);
            }
        } catch (Throwable $e) {
            $this->setErrorInfo(QueueException::ERR_MESG_CLIENT_CANNOT_CONNECT, QueueException::ERR_CODE_CLIENT_CANNOT_CONNECT);
        }

        return $res;
    }

    /**
     * 队列头移出元素
     * @return mixed
     */
    public function shift() {
        $res = null;

        /* @var $queInfo QueueInfo */
        $queInfo = $this->getQueueInfo();
        if (ValidateHelper::isEmptyObject($queInfo)) {
            return $res;
        }

        try {
            $client = $this->getRedisClient($this->connName);
            if ($this->len() == 0) {
                return $res;
            }

            if ($queInfo->isSort) {
                $arr = $client->zRange($queInfo->queueKey, 0, 0); //从小到大排
                if (!empty($arr)) {
                    $res = current($arr);
                }
            } else {
                $res = $client->lPop($queInfo->queueKey);
            }

            if ($res) {
                $arr     = $this->unpack(strval($res));
                $tranRes = $this->transfer($arr);
                $res     = ($arr && $tranRes) ? $arr : null;
            }
        } catch (Throwable $e) {
            $this->setErrorInfo(QueueException::ERR_MESG_CLIENT_CANNOT_CONNECT, QueueException::ERR_CODE_CLIENT_CANNOT_CONNECT);
        }

        return $res;
    }

    /**
     * 队列尾移出元素
     * @return mixed
     */
    public function pop() {
        $res = null;

        /* @var $queInfo QueueInfo */
        $queInfo = $this->getQueueInfo();
        if (ValidateHelper::isEmptyObject($queInfo)) {
            return $res;
        }

        try {
            $client = $this->getRedisClient($this->connName);
            if ($this->len() == 0) {
                return $res;
            }

            if ($queInfo->isSort) {
                $arr = $client->zRevRange($queInfo->queueKey, 0, 0); //从大到小排
                if (!empty($arr)) {
                    $res = current($arr);
                }
            } else {
                $res = $client->rPop($queInfo->queueKey);
            }

            if ($res) {
                $arr     = $this->unpack(strval($res));
                $tranRes = $this->transfer($arr);
                $res     = ($arr && $tranRes) ? $arr : null;
            }
        } catch (Throwable $e) {
            $this->setErrorInfo(QueueException::ERR_MESG_CLIENT_CANNOT_CONNECT, QueueException::ERR_CODE_CLIENT_CANNOT_CONNECT);
        }

        return $res;
    }

    /**
     * 获取单个消息的中转key
     * @param array $msg
     * @param string $queueName
     * @return string
     */
    public function getMsgToTransKey(array $msg, string $queueName = ''): string {
        if ($queueName == '') {
            $queueName = $this->queueName;
        }

        if (self::isWraped($msg)) {
            $msg = self::unwrapMsg($msg);
        }

        $data = [
            'queue' => $queueName,
            'msg'   => $msg,
        ];

        return md5(json_encode($data));
    }

    /**
     * 根据中转key获取单个消息
     * @param string $key 消息中转key
     * @param string $queueName 消息所在的队列名
     * @return array
     */
    public function getMsgByTransKey(string $key, string $queueName = ''): array {
        if (empty($key)) {
            return [];
        }

        if ($queueName == '') {
            $queueName = $this->queueName;
        }

        /* @var $queInfo QueueInfo */
        $queInfo = $this->getQueueInfo($queueName);
        if (ValidateHelper::isEmptyObject($queInfo)) {
            return [];
        }

        $tableKey = self::getTransTableKey($queInfo->priority);
        try {
            $client = $this->getRedisClient($this->connName);
            $str    = $client->hGet($tableKey, $key);
            if (!empty($str) && ValidateHelper::isJson(strval($str))) {
                $res = json_decode($str, true);
            }
        } catch (Throwable $e) {
            $this->setErrorInfo(QueueException::ERR_MESG_CLIENT_CANNOT_CONNECT, QueueException::ERR_CODE_CLIENT_CANNOT_CONNECT);
        }

        return isset($res) && !empty($res) ? $res : [];
    }

    /**
     * 获取多个消息的中转key
     * @param string $queueName 消息所在的队列名
     * @param array ...$msgs
     * @return array
     */
    public function getMsgsToTransKeys(string $queueName, array ...$msgs): array {
        if ($queueName == '') {
            $queueName = $this->queueName;
        }

        $res = [];
        foreach ($msgs as $msg) {
            if (self::isWraped($msg)) {
                $msg = self::unwrapMsg($msg);
            }

            $item = [
                'queue' => $queueName,
                'msg'   => $msg,
            ];
            $key  = md5(json_encode($item));
            array_push($res, $key);
        }

        return $res;
    }

    /**
     * 根据中转key获取多个消息
     * @param string $queueName 消息所在的队列名
     * @param string ...$keys
     * @return array
     */
    public function getMsgsByTransKeys(string $queueName, string ...$keys): array {
        if ($queueName == '') {
            $queueName = $this->queueName;
        }

        /* @var $queInfo QueueInfo */
        $queInfo = $this->getQueueInfo($queueName);
        if (ValidateHelper::isEmptyObject($queInfo)) {
            return [];
        }

        $res      = [];
        $tableKey = self::getTransTableKey($queInfo->priority);
        try {
            $client = $this->getRedisClient($this->connName);
            $arr    = $client->hMGet($tableKey, $keys);
            if (!empty($arr)) {
                foreach ($arr as $k => &$v) {
                    if (!empty($v) && ValidateHelper::isJson(strval($v))) {
                        $v = json_decode($v, true);
                    } else {
                        $v = [];
                    }
                }
            }
        } catch (Throwable $e) {
            $this->setErrorInfo(QueueException::ERR_MESG_CLIENT_CANNOT_CONNECT, QueueException::ERR_CODE_CLIENT_CANNOT_CONNECT);
        }

        return $res;
    }

    /**
     * 将消息加入中转队列
     * @param string $queueName
     * @param array $msg
     * @return bool
     */
    public function transfer(array $msg, string $queueName = ''): bool {
        // TODO: Implement transfer() method.
    }

    /**
     * 将中转消息重新加入相应的处理队列
     * @param int $transType 中转队列类型:0非优先队列中转,1优先队列中转
     * @param string $uniqueCode 机器唯一码
     * @return int
     */
    public function transMsgReadd2Queue(int $transType, string $uniqueCode = ''): int {
        // TODO: Implement transMsgReadd2Queue() method.
    }

    /**
     * 消息确认(处理完毕后向队列确认,成功则从中转队列移除;失败则重新加入任务队列;若无确认,消息重新入栈)
     * @param bool $ok 处理结果:true成功,false失败
     * @param mixed $msg 消息或该消息的中转key
     * @return bool
     */
    public function confirm(bool $ok, $msg): bool {
        // TODO: Implement confirm() method.
    }

    /**
     * 消息批量确认
     * @param bool $ok 处理结果:true成功,false失败
     * @param mixed ...$msgs 消息或该消息的中转key
     * @return int
     */
    public function confirmMulti(bool $ok, ...$msgs): int {
        // TODO: Implement confirmMulti() method.
    }

    /**
     * 获取队列长度
     * @param string $queueName 队列名
     * @return int
     */
    public function len(string $queueName = ''): int {
        // TODO: Implement len() method.
    }

    /**
     * 清空队列
     * @param string $queueName 队列名
     * @return bool
     */
    public function clear(string $queueName = ''): bool {
        // TODO: Implement clear() method.
    }

    /**
     * 删除队列
     * @param string $queueName 队列名
     * @return bool
     */
    public function delete(string $queueName = ''): bool {
        // TODO: Implement delete() method.
    }
}