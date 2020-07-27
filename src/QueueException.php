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

class QueueException extends BaseException {

    /**
     * redis客户端不存在-错误码
     */
    const ERR_CLIENT_NOTEXIST_CODE = 1;


    /**
     * redis客户端不存在-消息
     */
    const ERR_CLIENT_NOTEXIST_MSG = 'Redis client does not exist.';


    /**
     * 配置错误-错误码
     */
    const ERR_CONF_CODE = 2;


    /**
     * 配置错误-消息
     */
    const ERR_CONF_MSG = 'The fields configured must be the same as the default:';


    /**
     * redis不能连接-错误码
     */
    const ERR_CANNOT_CONNECT_CODE = 3;


    /**
     * redis不能连接-消息
     */
    const ERR_CANNOT_CONNECT_MSG = 'Cannot connect to Redis.';


    /**
     * 队列不存在-错误码
     */
    const ERR_QUEUE_NOTEXIST_CODE = 4;


    /**
     * 队列不存在-消息
     */
    const ERR_QUEUE_NOTEXIST_MSG = 'The queue does not exist.';


}