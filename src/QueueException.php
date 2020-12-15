<?php
/**
 * Created by PhpStorm.
 * User: Administrator
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
     * 错误码-队列存在但类型冲突
     */
    const ERR_CODE_QUEUE_EXIST_TYPECONFLICT = 3;


    /**
     * 错误消息-队列存在但类型冲突
     */
    const ERR_MESG_QUEUE_EXIST_TYPECONFLICT = 'The queue exist, but type conflict.';


    /**
     * 错误码-队列消息为空
     */
    const ERR_CODE_QUEUE_MESSAG_EEMPTY = 4;


    /**
     * 错误消息-队列消息为空
     */
    const ERR_MESG_QUEUE_MESSAG_EEMPTY = 'The queue message cannot be empty.';


    /**
     * 错误码-队列消息中转失败
     */
    const ERR_CODE_QUEUE_MESSAG_TRANSFERFAIL = 5;


    /**
     * 错误消息-队列消息中转失败
     */
    const ERR_MESG_QUEUE_MESSAG_TRANSFERFAIL = 'The queue message transfer failed.';


    /**
     * 错误码-队列消息确认失败
     */
    const ERR_CODE_QUEUE_MESSAG_CONFIRMFAIL = 6;


    /**
     * 错误消息-队列消息确认失败
     */
    const ERR_MESG_QUEUE_MESSAG_CONFIRMFAIL = 'The queue message confirm failed.';


    /**
     * 错误码-redis客户端不存在
     */
    const ERR_CODE_CLIENT_NOTEXIST = 7;


    /**
     * 错误消息-redis客户端不存在
     */
    const ERR_MESG_CLIENT_NOTEXIST = 'Redis client does not exist.';


    /**
     * 错误码-redis客户端不能连接
     */
    const ERR_CODE_CLIENT_CANNOT_CONNECT = 8;


    /**
     * 错误消息-redis客户端不能连接
     */
    const ERR_MESG_CLIENT_CANNOT_CONNECT = 'Redis client cannot connect.';


    /**
     * 错误码-队列操作失败
     */
    const ERR_CODE_QUEUE_OPERATE_FAIL = 99;


    /**
     * 错误消息-队列操作失败
     */
    const ERR_MESG_QUEUE_OPERATE_FAIL = 'The queue oeration failed, try again later.';


}