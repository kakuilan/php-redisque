# php-redisque
php redis queue,php轻量级redis队列库

### 相关
[![Php Version](https://img.shields.io/badge/php-%3E=7.2-brightgreen.svg)](https://secure.php.net/)
[![Build Status](https://travis-ci.org/kakuilan/php-redisque.svg?branch=master)](https://travis-ci.org/kakuilan/php-redisque)
[![codecov](https://codecov.io/gh/kakuilan/php-redisque/branch/master/graph/badge.svg)](https://codecov.io/gh/kakuilan/php-redisque)
[![Code Size](https://img.shields.io/github/languages/code-size/kakuilan/php-redisque.svg?style=flat-square)](https://github.com/kakuilan/php-redisque)
[![Starts](https://img.shields.io/github/stars/kakuilan/php-redisque.svg)](https://github.com/kakuilan/php-redisque)
[![Latest Version](https://img.shields.io/packagist/v/kakuilan/php-redisque.svg)](https://packagist.org/packages/kakuilan/php-redisque)

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
- 中转时间处理

### 测试
```sh
phpunit --bootstrap=tests/bootstrap.php ./tests/

# 或者
cd tests
phpunit
```
