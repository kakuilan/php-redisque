<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2020/7/18
 * Time: 20:04
 * Desc: Redis队列
 */

namespace Redisque;

use Kph\Services\BaseService;
use Kph\Consts;
use Kph\Helpers\ArrayHelper;
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
     * 所有队列名称的哈希表key
     */
    const QUEUE_ALL_NAME = 'all_queues';


    /**
     * 队列名
     * @var string
     */
    protected $name = '';


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


    public static function getPrefix(): string {
        // TODO: Implement getPrefix() method.
    }

    public static function getRedisDefault(): Redis {
        // TODO: Implement getRedisDefault() method.
    }

    public static function getQueues($redis = null): array {
        // TODO: Implement getQueues() method.
    }

    public static function countQueues($redis = null): int {
        // TODO: Implement countQueues() method.
    }

    public static function queueExists(string $queueName, $redis = null): bool {
        // TODO: Implement queueExists() method.
    }

    public static function getLockOperate(string $operation, int $dataId, int $operateUid, int $ttl = 60, $redis = null): int {
        // TODO: Implement getLockOperate() method.
    }

    public static function unlockOperate(string $operation, int $dataId, $redis = null): bool {
        // TODO: Implement unlockOperate() method.
    }

    public function newQueue(array $conf): QueueInterface {
        // TODO: Implement newQueue() method.
    }

    public function getRedisClient(string $connName = ''): Redis {
        // TODO: Implement getRedisClient() method.
    }

    public function add(array $msg): bool {
        // TODO: Implement add() method.
    }

    public function addMulti(array ...$msgs): bool {
        // TODO: Implement addMulti() method.
    }

    public function push(array $msg): bool {
        // TODO: Implement push() method.
    }

    public function pushMulti(array ...$msgs): bool {
        // TODO: Implement pushMulti() method.
    }

    public function shift() {
        // TODO: Implement shift() method.
    }

    public function pop() {
        // TODO: Implement pop() method.
    }

    public function transMsgReadd2Queue(int $transType, string $uniqueCode = ''): int {
        // TODO: Implement transMsgReadd2Queue() method.
    }

    public function getQueueInfo(string $queueName = ''): array {
        // TODO: Implement getQueueInfo() method.
    }

    public function getMsgToTransKey(array $msg): string {
        // TODO: Implement getMsgToTransKey() method.
    }

    public function getMsgByTransKey(string $key): array {
        // TODO: Implement getMsgByTransKey() method.
    }

    public function getMsgsToTransKeys(array ...$msg): array {
        // TODO: Implement getMsgsToTransKeys() method.
    }

    public function getMsgsByTransKeys(string ...$key): array {
        // TODO: Implement getMsgsByTransKeys() method.
    }

    public function transfer(array $msg): bool {
        // TODO: Implement transfer() method.
    }

    public function confirm(bool $ok, $msg): bool {
        // TODO: Implement confirm() method.
    }

    public function confirmMulti(bool $ok, ...$msgs): int {
        // TODO: Implement confirmMulti() method.
    }

    public function len(string $queueName = ''): int {
        // TODO: Implement len() method.
    }

    public function clear(string $queueName = ''): bool {
        // TODO: Implement clear() method.
    }

    public function delete(string $queueName = ''): bool {
        // TODO: Implement delete() method.
    }
}