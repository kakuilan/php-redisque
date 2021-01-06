<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2020/12/23
 * Time: 19:55
 * Desc: 队列信息
 */

namespace Redisque;

use Kph\Objects\StrictObject;

/**
 * Class QueueInfo
 * @package Redisque
 */
class QueueInfo extends StrictObject {


    /**
     * 队列名称
     * @var string
     */
    public $queueName = '';


    /**
     * 队列键名
     * @var string
     */
    public $queueKey = '';


    /**
     * 是否排序队列
     * @var bool
     */
    public $isSort = false;


    /**
     * 队列优先级
     * @var int
     */
    public $priority = 0;


    /**
     * 消息有效期,秒;默认0,永久
     * @var int
     */
    public $expire = 0;


}