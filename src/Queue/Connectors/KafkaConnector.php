<?php

namespace Rapide\LaravelQueueKafka\Queue\Connectors;

use Illuminate\Container\Container;
use Illuminate\Queue\Connectors\ConnectorInterface;
use Rapide\LaravelQueueKafka\Queue\KafkaQueue;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\Producer;
use RdKafka\TopicConf;

class KafkaConnector implements ConnectorInterface
{
    /**
     * @var Container
     */
    private $container;

    /**
     * KafkaConnector constructor.
     *
     * @param Container $container
     */
    public function __construct(Container $container)
    {
        $this->container = $container;
    }

    /**
     * Establish a queue connection.
     *
     * @param array $config
     *
     * @return \Illuminate\Contracts\Queue\Queue
     */
    public function connect(array $config)
    {
        $producerConf = $this->setConf([
            'bootstrap.servers' => $config['brokers'],
            'metadata.broker.list' => $config['brokers'],
        ]);

        $producer = app(Producer::class, ['conf' => $producerConf]);


        $topicConf = new TopicConf();
        $topicConf->set('auto.offset.reset', 'largest');



        $conf = [
            'group.id'=> $config['consumer_group_id'] ,
            'metadata.broker.list'=> $config['brokers'],
            'enable.auto.commit'=> 'false',
        ];

        if(true === $config['sasl_enable']) {
            $conf = $conf + [
                    'sasl.mechanisms'=> 'PLAIN',
                    'sasl.username'=> $config['sasl_plain_username'],
                    'sasl.password'=> $config['sasl_plain_password'],
                    'ssl.ca.location'=> $config['ssl_ca_location'],
                ];
        }

        $consumerConf = $this->setConf($conf);
        $consumerConf->setDefaultTopicConf($topicConf);


        $consumer = app(\RdKafka\Consumer::class, [
            'conf' => $consumerConf,
        ]);

        return new KafkaQueue(
            $producer,
            $consumer,
            $config
        );
    }

    private function setConf(array $options): Conf
    {
        $conf = new Conf();

        foreach ($options as $key => $value) {
            $conf->set($key, $value);
        }

        return $conf;
    }
