use tonic;
tonic::include_proto!("clickhouse.grpc");

pub type Client = click_house_client::ClickHouseClient<tonic::transport::channel::Channel>;
