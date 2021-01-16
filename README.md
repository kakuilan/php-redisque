# php-redisque
php redis queue,php轻量级redis队列库

### usage
```shell script
#安装
composer require kakuilan/php-redisque

```

### 须重写方法
- `RedisQueue::getDefaultRedis`
- `RedisQueue::getRedisClient`
- 

### TODO
- 2个中转队列,一个快,一个慢
- 中转队列是个有序集合,重要的消息排在前面
