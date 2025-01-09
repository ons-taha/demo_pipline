   CREATE TABLE datawarehouse.traunscation_loc ON CLUSTER '{cluster}'
(
transaction_id                     UUID CODEC(LZ4),
customer_id                        UUID CODEC(ZSTD(3)),
card_type                          LowCardinality(String),
amount                             UInt64 CODEC(T64),                               -- deteremine base on business amount range study if there - negative balance or not and and what the highest amount could be in both side
transaction_type                               LowCardinality(String),                         --withdraw\deposite \......
vender_id                          UUID CODEC(ZSTD(3)),                                -- determin if with purshase inside app vender ,sister company, or external
date_time                          DateTime64(9, 'UTC') Codec(DoubleDelta ,LZ4),   --transaction event time
session_id                         UUID CODEC(LZ4),                                -- determine if used app or  not for trunscation(null='' if not useed app)
ip_address                         IPv4 CODEC(Delta, ZSTD(3)),
city                               LowCardinality(String),
snap_shot                          Date Codec(DoubleDelta ,LZ4)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/{shard}/{table}', '{replica}')
PARTITION BY toYYYYMM(date_time)
ORDER BY (date_time);


CREATE TABLE datawarehouse.traunscation
ON CLUSTER '{cluster}' as datawarehouse.traunscation_loc
ENGINE = Distributed('{cluster}',datawarehouse,traunscation_loc, rand());
