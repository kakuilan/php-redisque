<?php
/**
 * Copyright (c) 2020 LKK All rights reserved
 * User: kakuilan
 * Date: 2021/1/21
 * Time: 18:09
 * Desc:
 */

define('DS', str_replace('\\', '/', DIRECTORY_SEPARATOR));
define('PS', PATH_SEPARATOR);
define('TESTDIR', str_replace('\\', '/', __DIR__ . DS));
error_reporting(E_ALL);
ini_set('display_errors', 0);

$loader  = require __DIR__ . '/../vendor/autoload.php';
$logFile = TESTDIR . date('Ymd') . '.log';
register_shutdown_function('\Kph\Helpers\DebugHelper::errorLogHandler', $logFile);