<?php
/**
 * Created by PhpStorm.
 * User: Administrator
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
     * 有序列表的分数字段名
     */
    const QUEUE_SCORE_FIELD = 'queue_score';


    /**
     * 所有队列名称的哈希表 ['队列名'=>'队列的键']
     */
    const QUEUE_ALL_NAME = 'all_queues';


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
     * 中转队列重新入栈时间
     * @var int
     */
    protected $transTime = 0;


    /**
     * redis连接名
     * @var string
     */
    protected $connName = '';


    /**
     * 队列名缓存
     * @var array
     */
    protected static $queueNameCaches = [];


    /**
     * 获取键前缀
     * @return string
     */
    public static function getPrefix(): string {
        $res = static::getClass() . ':';
        return str_replace('\\', '-', $res);
    }


    /**
     * 获取默认的redis客户端连接
     * @return Redis
     * @throws Throwable
     */
    public static function getRedisDefault(): Redis {
        return RedisConn::getRedis([]);
    }


    /**
     * 获取队列名哈希表名
     * @return string
     */
    protected static function getQueueTableKey(): string {
        return self::getPrefix() . self::QUEUE_ALL_NAME;
    }


    /**
     * 获取所有队列名
     * @param null|mixed $redis Redis客户端对象
     * @return array
     */
    public static function getQueues($redis = null): array {
        if (empty($redis)) {
            try {
                $redis = self::getRedisDefault();
            } catch (Throwable $e) {
            }
        }
        if (!is_object($redis) || !($redis instanceof Redis)) {
            return [];
        }

        $key = self::getQueueTableKey();
        $res = $redis->hGetAll($key);


        return empty($res) ? [] : (array)$res;
    }


    /**
     * 统计队列数
     * @param null|mixed $redis Redis客户端对象
     * @return int
     */
    public static function countQueues($redis = null): int {
        if (empty($redis)) {
            try {
                $redis = self::getRedisDefault();
            } catch (Throwable $e) {
            }
        }
        if (!is_object($redis) || !($redis instanceof Redis)) {
            return 0;
        }

        $key = self::getQueueTableKey();
        return (int)$redis->hLen($key);
    }


    /**
     * 检查队列是否存在
     * @param string $queueName 队列名
     * @param null|mixed $redis Redis客户端对象
     * @return bool
     */
    public static function queueExists(string $queueName, $redis = null): bool {
        if (empty($redis)) {
            try {
                $redis = self::getRedisDefault();
            } catch (Throwable $e) {
            }
        }
        if (!is_object($redis) || !($redis instanceof Redis)) {
            return false;
        }

        $key = self::getQueueTableKey();
        return (bool)$redis->hExists($key, $queueName);
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
                $redis = self::getRedisDefault();
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
                $redis = self::getRedisDefault();
            } catch (Throwable $e) {
            }
        }
        if (!is_object($redis) || !($redis instanceof Redis)) {
            return $res;
        }

        $key = self::getOperateKey($operation, $dataId);
        $res = (int)$redis->del($key);

        return $res;
    }


    /**
     * 新建队列/设置队列
     * @param array $conf 队列配置
     * @return $this
     * @throws Throwable
     */
    public function newQueue(array $conf): QueueInterface {
        $queueName = StringHelper::trim($conf['queueName'] ?? '');
        $connName  = StringHelper::trim($conf['connName'] ?? '');
        $isSort    = boolval($conf['isSort'] ?? false);
        $priority  = intval($conf['priority'] ?? 0);
        $expire    = intval($conf['expire'] ?? 0);
        $transTime = intval($conf['transTime'] ?? 0);
        $priority  = $priority ? 1 : 0;

        if ($expire < 0) {
            $expire = Consts::TTL_DEFAULT;
        }

        if ($queueName == '') {
            throw new BaseException(QueueException::ERR_MESG_QUEUE_NAMEEMPTY, QueueException::ERR_CODE_QUEUE_NAMEEMPTY);
        }

        $sortType = $isSort ? self::QUEUE_TYPE_ISSORT : self::QUEUE_TYPE_NOSORT;


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

        return self::getRedisDefault();
    }


    /**
     * 获取队列信息
     * @param string $queueName 队列名
     * @return array
     */
    public function getQueueInfo(string $queueName = ''): array {
        // TODO: Implement getQueueInfo() method.
    }


    /**
     * 队列头压入一个消息
     * @param array $msg
     * @return bool
     */
    public function add(array $msg): bool {
        // TODO: Implement add() method.
    }

    /**
     * 队列头压入多个个消息
     * @param array ...$msgs
     * @return bool
     */
    public function addMulti(array ...$msgs): bool {
        // TODO: Implement addMulti() method.
    }

    /**
     * 队列尾压入一个消息
     * @param array $msg
     * @return bool
     */
    public function push(array $msg): bool {
        // TODO: Implement push() method.
    }

    /**
     * 队列尾压入多个消息
     * @param array ...$msgs
     * @return bool
     */
    public function pushMulti(array ...$msgs): bool {
        // TODO: Implement pushMulti() method.
    }

    /**
     * 队列头移出元素
     * @return mixed
     */
    public function shift() {
        // TODO: Implement shift() method.
    }

    /**
     * 队列尾移出元素
     * @return mixed
     */
    public function pop() {
        // TODO: Implement pop() method.
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
     * 获取单个消息的中转key
     * @param array $msg
     * @return string
     */
    public function getMsgToTransKey(array $msg): string {
        // TODO: Implement getMsgToTransKey() method.
    }

    /**
     * 根据中转key获取单个消息
     * @param string $key
     * @return array
     */
    public function getMsgByTransKey(string $key): array {
        // TODO: Implement getMsgByTransKey() method.
    }

    /**
     * 获取多个消息的中转key
     * @param array ...$msg
     * @return array
     */
    public function getMsgsToTransKeys(array ...$msg): array {
        // TODO: Implement getMsgsToTransKeys() method.
    }

    /**
     * 根据中转key获取多个消息
     * @param string ...$key
     * @return array
     */
    public function getMsgsByTransKeys(string ...$key): array {
        // TODO: Implement getMsgsByTransKeys() method.
    }

    /**
     * 将消息加入中转队列
     * @param array $msg
     * @return bool
     */
    public function transfer(array $msg): bool {
        // TODO: Implement transfer() method.
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