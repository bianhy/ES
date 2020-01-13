<?php
/**
 * Created by PhpStorm.
 * User: bianhy
 * Date: 2019/8/9
 * Time: 14:58
 */

namespace App\Controller;

use Amber\System\Libraries\Database\DB;
use Elasticsearch\ClientBuilder;

class ElasticSearchController extends AbstractController
{
    protected $client;
    protected $index_name = 'teacher_message';
    protected $type_name  = 'doc';

    private $dsl = [
        'query' => [
            'match_all' => [],
        ],
    ];

    protected $mapping = [
        'properties' => [
            'id' => [
                'type' => 'long',
                'index' => true,
            ],
            'data_source' => [
                'type' => 'integer',
                'index' => true,
            ],
            'type' => [
                'type' => 'integer',
                'index' => true,
            ],
            'user_id' => [
                'type' => 'long',
                'index' => true,
            ],
            'account_id' => [
                'type' => 'long',
                'index' => true,
            ],
            'user_real_name' => [
                'type' => 'keyword',
                'index' => true,
            ],
            'subject_id' => [
                'type' => 'integer',
                'index' => true,
            ],
            'teacher_id' => [
                'type' => 'long',
                'index' => true,
            ],
            'item_id' => [
                'type' => 'keyword',
                'index' => true,
            ],
            'params' => [
                'type' => 'keyword',
                'index' => true,
            ],
            'content' => [
                'type' => 'keyword',
                'index' => true,
            ],
            'status' => [
                'type' => 'integer',
                'index' => true,
            ],
            'create_dt' => [
                'type' => 'date',
                'index' => true, //精准查找
                'format' => 'yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis',
            ],
            'update_dt' => [
                'type' => 'date',
                'index' => true, //精准查找
                'format' => 'yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis',
            ],
        ],
    ];

    protected $settings = [
        //es默认配置是10000条，导致查询10000条之后报错
        'max_result_window'  => 2000000000,
    ];

    public function __construct()
    {
        parent::__construct();
        $clientBuilder = ClientBuilder::create();
        $clientBuilder->setHosts(['localhost:9200']);
        $clientBuilder->setRetries(2);
        $this->client = $clientBuilder->build();
    }

    public function create()
    {

        $message = [
            'id' => '236',
            'data_source' => 1,
            'type' => 1,
            'user_id' => 6860100823373426,
            'account_id' => 8880100823373425,
            'user_real_name' => '宇文一二',
            'subject_id' => 3,
            'teacher_id' => 3140100823358888,
            'item_id' => '67607',
            'content' => '宇文一二在东巷右大街吃了一碗面。。。',
            'status' => 1,
            'create_dt' => '2019-05-28 11:16:54',
            'update_dt' => '2019-07-15 19:35:54',
        ];

        $index_exist = $this->checkIndex($this->index_name);

        if(!$index_exist){
            $this->createIndex($this->index_name,$this->type_name,$this->mapping);
        }

        //写入之前先检测下是否已写入
        $message_exist = $this->getDocument($message['id']);

        if(!$message_exist){
            $ret = $this->setDocument($this->index_name,$this->type_name,$message['id'],$message);
            var_dump($ret);exit;
        } else {
            $this->updateDocument($this->index_name,$this->type_name,$message['id'],$message);
        }
        echo 123;
        return true;
    }

    /**
     * 批量写入ES脚本
     * /opt/app/php-5.6.20/bin/php /data/wwwroot/Elasticsearch/app/www/index.php -c='App\Controller\ElasticSearchController' -a=bulk
     *  windows: 进入www目录
     *  php index.php -c=App\Controller\ElasticSearchController -a=bulk
     */
    public function bulk()
    {
        $start_time = microtime(1);
        $index_exist = $this->checkIndex($this->index_name);

        if($index_exist){
            //清空数据，再新建索引
            $this->deleteIndex($this->index_name);
        }

        $this->createIndex($this->index_name,$this->type_name,$this->mapping);

        $max = DB::table('message')->max('id');

        $size  = 2000;
        $count = ceil($max / $size);

        echo '数据写入开始,共执行'.$count.'次'.PHP_EOL;

        for ($i = 1;$i<=$count;$i++){
            $list = DB::table('message')->whereBetween('id',[($i - 1) * $size + 1,$i * $size])->get();

            echo '第'.$i.'页,每页'.$size.'条'.PHP_EOL;

            $params = [];
            foreach ($list as $value) {
                $params['body'][] = [
                    'index' => [
                        '_index' => $this->index_name,
                        '_type'  => $this->type_name,
                        '_id'    => $value['id'],
                    ],
                ];
                $params['body'][] = $value;
            }
            try{
                $this->client->bulk($params);
            }catch (\Exception $e){
                echo '执行失败，跳过。失败原因：'.json_decode($e->getMessage(), 1).PHP_EOL;
                continue;
            }

            echo '执行完成'.PHP_EOL;
        }

        echo '写入完成，共执行：'.$max.'条数据，耗时：'.(microtime(1) - $start_time). '秒'.PHP_EOL;
    }


    /**
     * 检测索引是否存在
     * @param $index
     * @return bool
     */
    protected function checkIndex($index)
    {
        $params = [
            'index' => $index
        ];
        return $this->client->indices()->exists($params);
    }

    /**
     * 创建索引
     * @param $index
     * @param $type
     * @param array $mappings
     * @return array
     */
    protected function createIndex($index,$type,$mappings = [])
    {
        $params = [
            'index' => $index,
            //'type'  => $type,
        ];
        if ($mappings){
            $params['body'] = [
                'mappings' => [
                    $type => $mappings
                ],
                'settings' => $this->settings
            ];
        }
        return $this->client->indices()->create($params);
    }

    /**
     * 删除索引
     * @param $index
     * @return array
     */
    protected function deleteIndex($index)
    {
        $params = [
            'index' => $index,
        ];
        return $this->client->indices()->delete($params);
    }

    /**
     * 根据ID获取单条消息
     * @param int $id
     * @return bool | array
     */
    protected function getDocument($id)
    {
        $params = [
            'index' => $this->index_name,
            'type'  => $this->type_name,
            'id'    => $id
        ];
        try{
            $ret = $this->client->get($params);
        }catch (\Exception $e){
            $ret = false;
        }
        return $ret ? $ret['_source'] : $ret;
    }

    /**
     * 写入单条消息
     * @param $index
     * @param $type
     * @param $id
     * @param $body
     * @return array|bool
     */
    protected function setDocument($index,$type,$id,$body)
    {
        $params = [
            'index' => $index,
            'type'  => $type,
            'id'    => $id,
            'body'  => $body
        ];

        try{
            $ret = $this->client->index($params);

        }catch (\Exception $e){
            //var_dump(json_decode($e->getMessage(), 1));exit;
            $ret = false;
        }
        return $ret;
    }
    /**
     * 更新单条消息
     * @param $index
     * @param $type
     * @param $id
     * @param $body
     * @return array|bool
     */
    protected function updateDocument($index,$type,$id,$body)
    {
        $params = [
            'index' => $index,
            'type'  => $type,
            'id'    => $id,
            'body'  => ['doc'=>$body]
        ];
        //var_dump($params);exit;
        try{
            $ret = $this->client->update($params);
        }catch (\Exception $e){
            //var_dump(json_decode($e->getMessage(), 1));exit;
            $ret = false;
        }
        return $ret;
    }

    /**
     * 删除单条消息
     * @param $index
     * @param $type
     * @param $id
     * @param $body
     * @return array|bool
     */
    protected function delDocument($index,$type,$id)
    {
        $params = [
            'index' => $index,
            'type'  => $type,
            'id'    => $id,
        ];
        //var_dump($params);exit;
        try{
            $ret = $this->client->delete($params);
        }catch (\Exception $e){
            //var_dump(json_decode($e->getMessage(), 1));exit;
            $ret = false;
        }
        return $ret;
    }



    public function namespace1()
    {
        return 'indices() 	    索引数据统计和显示索引信息
                 nodes() 	    节点数据统计和显示节点信息
                 cluster() 	    集群数据统计和显示集群信息
                 snapshot() 	对集群和索引进行产生快照或恢复数据
                 cat() 	        执行 Cat API （通常在命令使用）';
    }


    /**
     * 获取mapping
     * @return array
     */
    public function getMapping()
    {
        $params = [
            'index' => 'video_drag',
        ];
        $exist = $this->client->indices()->exists($params);
        try {
            $map = $this->client->indices()->getMapping($params);
        } catch (\Exception $e) {
            var_dump(json_decode($e->getMessage(), 1));
            exit;
        }

        return $map;

    }

    public function search()
    {
        //video_drag/kpi
        //no_action/kpi
        //video_end_daze/kpi
        //video_pause_daze/kpi
        $params = [
            'index' => 'video_end_daze',
            'type' => '_doc',
        ];

        $response = $this->client->search($params);
        print_r($response);
    }



    public function test()
    {

        $options = [
            'teacher_id' => 7297,
        ];

        $formatSearch = $this->formatSearch($options);

        $query = $formatSearch['params'] ?? [];
        //如果检索条件为空则处理全部
        if (empty($query)) {
            $query = [];
            $query['match_all'] = [];
        }

        //处理排序
        $sort_type = $options['sort_type'] ?? 'id';
        $sort_type = strtolower($sort_type);
        if (!in_array($sort_type, ['asc', 'desc'])) {
            $sort_type = 'asc';
        }

        $this->dsl['sort'][] = ['id' => ['order' => 'asc']];
        $this->dsl['query'] = $query;
        $this->dsl['from'] = 0;
        $this->dsl['size'] = 20;
        //var_dump($this->dsl);exit;
        $body = json_encode($this->dsl);
        $body = str_replace("[]", "{}", $body);
        $this->dsl = [
            'query' => [
                'match_all' => [],
            ],
        ];
        $params = [
            'index'=> $this->index_name,
            'type' => $this->type_name,
            'body' => $body
        ];
        //var_dump($params);exit;
        $response = $this->client->search($params);
        var_dump($response);exit;
    }

    private function formatSearch($options = [])
    {

        $params = [];
        $ser = []; //全局匹配

        if (isset($options['data_source']) && !empty($options['data_source'])) {
            $ser[] = ["term" => ["data_source" => $options['data_source']]];
        }

        if (isset($options['type']) && !empty($options['type'])) {
            $ser[] = ["term" => ["type" => $options['type']]];
        }

        if (isset($options['user_id']) && !empty($options['course_id'])) {
            $ser[] = ["term" => ["course_id" => $options['course_id']]];
        }

        if (isset($options['account_id']) && !empty($options['account_id'])) {
            $ser[] = ["term" => ["account_id" => $options['account_id']]];
        }

        if (isset($options['user_real_name']) && !empty($options['user_real_name'])) {
            $ser[] = ["term" => ["user_real_name" => $options['user_real_name']]];
        }

        if (isset($options['subject_id']) && !empty($options['subject_id'])) {
            $ser[] = ["term" => ["subject_id" => $options['subject_id']]];
        }

        if (isset($options['teacher_id']) && !empty($options['teacher_id'])) {
            $ser[] = ["term" => ["teacher_id" => $options['teacher_id']]];
        }

        if (isset($options['item_id']) && !empty($options['item_id'])) {
            $ser[] = ["term" => ["item_id" => $options['item_id']]];
        }

        if (isset($options['status']) && !empty($options['status'])) {
            $ser[] = ["term" => ["status" => $options['status']]];
        }

        //按时间查询
        if (isset($options['create_dt']) && !empty($options['create_dt'])) {
            $s_created = date("Y-m-d", strtotime($options['create_dt'])) . " 00:00:00";
            if (isset($options['end_dt']) && !empty($options['end_dt'])) {
                $e_created = date("Y-m-d", strtotime($options['end_dt'])) . " 23:59:59";
            } else {
                $e_created = date("Y-m-d", time()) . " 23:59:59";
            }
            $ser[] = ["range" => ["create_dt" => ['gte' => $s_created, 'lte' => $e_created]]];
        }

        if (!empty($ser)) {
            $params['bool']['must'][] = $ser;
        }

        return ['params' => $params];
    }
}
