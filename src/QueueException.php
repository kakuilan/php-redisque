<?php
/**
 * Copyright (c) 2020 LKK All rights reserved
 * User: kakuilan
 * Date: 2020/7/18
 * Time: 21:40
 * Desc: 队列异常
 */

namespace Redisque;

use Kph\Exceptions\BaseException;


/**
 * Class QueueException
 * @package Redisque
 */
class QueueException extends BaseException {


    /**
     * 错误码-队列名为空
     */
    const ERR_CODE_QUEUE_NAMEEMPTY = 1;


    /**
     * 错误消息-队列名为空
     */
    const ERR_MESG_QUEUE_NAMEEMPTY = 'The queue name cannot be empty.';


    /**
     * 错误码-队列不存在
     */
    const ERR_CODE_QUEUE_NOTEXIST = 2;


    /**
     * 错误消息-队列不存在
     */
    const ERR_MESG_QUEUE_NOTEXIST = 'The queue does not exist:';


    /**
     * 错误码-获取队列信息失败
     */
    const ERR_CODE_QUEUE_INFO_FAIL = 3;


    /**
     * 错误消息-获取队列信息失败
     */
    const ERR_MESG_QUEUE_INFO_FAIL = 'Get the queue info failed:';


    /**
     * 错误码-队列存在但类型冲突
     */
    const ERR_CODE_QUEUE_EXIST_TYPECONFLICT = 4;


    /**
     * 错误消息-队列存在但类型冲突
     */
    const ERR_MESG_QUEUE_EXIST_TYPECONFLICT = 'The queue exist, but type conflict.';


    /**
     * 错误码-队列消息为空
     */
    const ERR_CODE_QUEUE_MESSAG_EMPTY = 5;


    /**
     * 错误消息-队列消息为空
     */
    const ERR_MESG_QUEUE_MESSAG_EMPTY = 'The queue message cannot be empty.';


    /**
     * 错误码-队列消息中转失败
     */
    const ERR_CODE_QUEUE_MESSAG_TRANSFERFAIL = 6;


    /**
     * 错误消息-队列消息中转失败
     */
    const ERR_MESG_QUEUE_MESSAG_TRANSFERFAIL = 'The queue message transfer failed.';


    /**
     * 错误码-队列消息确认失败
     */
    const ERR_CODE_QUEUE_MESSAG_CONFIRMFAIL = 7;


    /**
     * 错误消息-队列消息确认失败
     */
    const ERR_MESG_QUEUE_MESSAG_CONFIRMFAIL = 'The queue message confirm failed.';


    /**
     * 错误码-队列-中转队列为空
     */
    const ERR_CODE_QUEUE_TRANS_QUEEMPTY = 8;


    /**
     * 错误码-队列-中转队列为空
     */
    const ERR_MESG_QUEUE_TRANS_QUEEMPTY = 'The transfer queue is empty.';


    /**
     * 错误码-redis客户端不存在
     */
    const ERR_CODE_CLIENT_NOTEXIST = 9;


    /**
     * 错误消息-redis客户端不存在
     */
    const ERR_MESG_CLIENT_NOTEXIST = 'Redis client does not exist.';


    /**
     * 错误码-redis客户端不能连接
     */
    const ERR_CODE_CLIENT_CANNOT_CONNECT = 10;


    /**
     * 错误消息-redis客户端不能连接
     */
    const ERR_MESG_CLIENT_CANNOT_CONNECT = 'Redis client cannot connect.';


    /**
     * 错误码-redis客户端获取锁失败
     */
    const ERR_CODE_CLIENT_LOCK_FAIL = 11;


    /**
     * 错误消息-redis客户端获取锁失败
     */
    const ERR_MESG_CLIENT_LOCK_FAIL = 'Redis client get lock fail.';


    /**
     * 错误码-队列操作失败
     */
    const ERR_CODE_QUEUE_OPERATE_FAIL = 99;


    /**
     * 错误消息-队列操作失败
     */
    const ERR_MESG_QUEUE_OPERATE_FAIL = 'The queue oeration failed, try again later.';


}