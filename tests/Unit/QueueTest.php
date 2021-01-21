<?php
/**
 * Copyright (c) 2021 LKK All rights reserved
 * User: kakuilan
 * Date: 2021/1/21
 * Time: 19:08
 * Desc:
 */

namespace Redisque\Tests\Unit;

use PHPUnit\Framework\TestCase;
use Error;
use Exception;
use Throwable;
use Redisque\RedisConn;
use Redisque\RedisClient;
use Redisque\RedisQueue;

class QueueTest extends TestCase {

    public function testGetPrefix() {
        $str = RedisQueue::getPrefix();
        $this->assertNotEmpty($str);
    }


}