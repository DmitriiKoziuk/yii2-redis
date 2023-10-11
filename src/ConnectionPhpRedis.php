<?php

namespace yii\redis;

use Redis;

class ConnectionPhpRedis extends Connection
{
    /** @var null|Redis  */
    private $redis = null;

    public function open()
    {
        if (null === $this->redis) {
            $this->redis = new Redis();
            $this->redis->connect($this->hostname, $this->port);
            $this->redis->auth([$this->username, $this->password]);
            $this->redis->select($this->database);
        }

        parent::open();
    }

    public function executeCommand($name, $params = [])
    {
        $this->open();

        if (false === \in_array($name, ['HGET', 'HGETALL', 'FT.SEARCH', 'FT.AGGREGATE'])) {
            return parent::executeCommand($name, $params);
        }

        \Yii::trace("Executing Redis Command: {$name}", __METHOD__);

        $data = [];
        if (true === \method_exists($this->redis, $name)) {
            $rawData = $this->redis->$name(...$params);
            if ('HGETALL' === $name) {
                foreach ($rawData as $field => $value) {
                    $data[] = $field;
                    $data[] = $value;
                }
            } else {
                $data = $rawData;
            }
        } else {
            $data = $this->redis->rawCommand($name, ...$params);
        }

        return $data;
    }

    public function pipeline(): Redis {
        $this->open();

        return $this->redis->pipeline();
    }
}
