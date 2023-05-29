# storage_opendal

`CITA-Cloud`中[storage微服务](https://github.com/cita-cloud/cita_cloud_proto/blob/master/protos/storage.proto)的实现，基于[opendal](https://github.com/apache/incubator-opendal)。

## 编译docker镜像
```
docker build -t citacloud/storage_opendal .
```

## 使用方法

```
$ storage -h
storage 6.7.0
Rivtower Technologies <contact@rivtower.com>

Usage: storage <COMMAND>

Commands:
  run   run this service
  help  Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

### storage-run

运行`storage`服务。

```
$ storage run -h
run this service

Usage: storage run [OPTIONS]

Options:
  -c, --config <CONFIG_PATH>  Chain config path [default: config.toml]
  -h, --help                  Print help
```

参数：
1. `config` 微服务配置文件。

    参见示例`example/config.toml`。

    其中`[storage_opendal]`段为微服务的配置：
    * `storage_port` 为本微服务的`gRPC`服务监听的端口号
    * `domain` 节点的域名
    * `enable_metrics` 为`metrics`功能开关
    * `l1_capacity` 为一级存储的容量
    * `l2_capacity` 为二级存储的容量

    其中`[storage_opendal.log_config]`段为微服务日志的配置：
    * `max_level` 日志等级
    * `filter` 日志过滤配置
    * `service_name` 服务名称，用作日志文件名与日志采集的服务名称
    * `rolling_file_path` 日志文件路径
    * `agent_endpoint` jaeger 采集端地址

    其中`[storage_opendal.cloud_storage]`段为第三层S3云存储服务的配置，如不配置则不开启第三层：
    * `endpoint` 为S3服务的地址
    * `access_key_id` 为S3服务的access_key_id
    * `secret_access_key` 为S3服务的secret_access_key
    * `bucket` 为S3服务的bucket名称

```
$ storage run -c example/config.toml
2023-03-29T17:25:16.300670392+08:00  INFO storage: storage grpc port: 60003
2023-03-29T17:25:16.300717222+08:00  INFO storage: storager data root: chain_data
2023-03-29T17:25:16.359770959+08:00  INFO storage::storager: build storager: layer: 2, scheme: rocksdb
2023-03-29T17:25:16.359830222+08:00  INFO storage::storager: build storager: layer: 1, scheme: memory
2023-03-29T17:25:16.359837265+08:00  INFO storage: start storage_opendal grpc server
2023-03-29T17:25:16.359842175+08:00  INFO storage: metrics off
```

## 设计

storage_opendal是基于opendal开发的多层存储服务。

总共分三层：
1. 第一层为内存存储。读写速度极快，容量小，不能持久化，用于缓存数据。
2. 第二层为rocksdb存储。读写速度快，容量大，可以持久化，用于持久化数据。
3. 第三层为S3云存储。读写速度慢，持久化全量数据。

如果在配置文件中添加了第三层的相关配置，则会开启第三层，全量数据将备份到第三层；若没有相关配置则只会启用两层存储，全量数据存储在第二层，此时配置中的`l2_capacity`和`backup_interval`未使用。

当收到一个存储请求时，会连续将数据存储到第一层和第二层，第二层完成操作后将结果返回给调用者。如果启用了第三层，则每隔一段时间会执行一次备份任务，将第二层的新数据备份到第三层。

当收到一个读取请求时，会逐层尝试读取数据，读取成功则立即返回结果，读取失败则尝试从下一层读取，直至最后一层，若仍然失败则返回失败。

第一层每次存储新区块时会将过时的区块数据从第一层删除；第二层每次执行完备份任务时会将过时的区块数据从第二层删除。


## 连续读写对比测试

与storage_rocksdb进行连续读写测试对比。

规则为：连续进行10000次操作，在执行1000次后开始统计，每3000次为一轮记录耗时，最后取三轮的平均值。

由数据可以得出结论：storage_opendal第二层的连续读写速度与原storage_rocksdb相当。
### store
storage_rocksdb
```
bench store start!
        round: 1 finish: spend: 1203.636652 ms, store 3000 times
        round: 2 finish: spend: 1214.252722 ms, store 3000 times
        round: 3 finish: spend: 1219.958407 ms, store 3000 times
bench store finish! speed: 2473.990266169413 req/s, 0.40420530900000007 ms/req
```

storage_opendal layer1
```
bench store start!
        round: 1 finish: spend: 1151.66794 ms, store 3000 times
        round: 2 finish: spend: 1104.428591 ms, store 3000 times
        round: 3 finish: spend: 1115.832889 ms, store 3000 times
bench store finish! speed: 2669.0950132639487 req/s, 0.3746588244444445 ms/req
```

storage_opendal layer2
```
bench store start!
        round: 1 finish: spend: 1225.493029 ms, store 3000 times
        round: 2 finish: spend: 1229.877721 ms, store 3000 times
        round: 3 finish: spend: 1200.959682 ms, store 3000 times
bench store finish! speed: 2461.484312586331 req/s, 0.4062589368888888 ms/req
```

storage_opendal layer3
```
bench store start!
        round: 1 finish: spend: 418907.020785 ms, store 3000 times
        round: 2 finish: spend: 439741.098185 ms, store 3000 times
        round: 3 finish: spend: 380956.621735 ms, store 3000 times
bench store finish! speed: 7.260378816300293 req/s, 137.73386007833332 ms/req
```

结果如下表所示：
| Storage Service | Layer | Speed (req/s)| Latency (ms/req) |
| --------------- | ----- | -------------| ---------------- |
| rocksdb         | 1     | 2473.99      | 0.40             |
| opendal         | 1     | 2669.09      | 0.37             |
| opendal         | 2     | 2461.48      | 0.40             |
| opendal         | 3     | 7.26         | 137.73           |

### load
storae_rocksdb
```
bench load layer1 start!
        layer1 round: 1 finish: spend: 1142.0315329999999 ms, store 3000 times
        layer1 round: 2 finish: spend: 1166.3740739999998 ms, store 3000 times
        layer1 round: 3 finish: spend: 1157.0330310000002 ms, store 3000 times
bench load layer1 finish! speed: 2597.073831090586 req/s, 0.38504873755555546 ms/req
```

storage_opendal layer1
```
bench load layer1 start!
        layer1 round: 1 finish: spend: 1159.674532 ms, store 3000 times
        layer1 round: 2 finish: spend: 1126.45983 ms, store 3000 times
        layer1 round: 3 finish: spend: 1136.057658 ms, store 3000 times
bench load layer1 finish! speed: 2629.893339532713 req/s, 0.38024355777777774 ms/req
```

storage_opendal layer2
```
bench load layer2 start!
        layer2 round: 1 finish: spend: 1167.892932 ms, store 3000 times
        layer2 round: 2 finish: spend: 1178.769264 ms, store 3000 times
        layer2 round: 3 finish: spend: 1164.5414030000002 ms, store 3000 times
bench load layer2 finish! speed: 2563.2236201179626 req/s, 0.3901337332222223 ms/req
```

storage_opendal layer3
```
bench load layer3 start!
        layer3 round: 1 finish: spend: 39799.035638 ms, store 3000 times
        layer3 round: 2 finish: spend: 40837.275716000004 ms, store 3000 times
        layer3 round: 3 finish: spend: 41356.491394000004 ms, store 3000 times
bench load layer3 finish! speed: 73.77484406675417 req/s, 13.55475586088889 ms/req
```

| Storage Service | Layer | Speed (req/s)| Latency (ms/req) |
| --------------- | ----- | -------------| ---------------- |
| rocksdb         | 1     | 2597.07      | 0.38             |
| opendal         | 1     | 2629.89      | 0.38             |
| opendal         | 2     | 2563.22      | 0.39             |
| opendal         | 3     | 73.77        | 13.55            |
