use crate::config::Admin;

use warp;
use warp::Filter;
use warp::filters::path::FullPath;
use warp::http::{Method, Response};
use byte_unit::Byte as ByteSize;
use base64::{Engine as _, engine::general_purpose};
use std::collections::HashSet;
use std::convert::Infallible;
use std::io::prelude::*;
use std::net::{SocketAddr, IpAddr};
use std::sync::Arc;
use bytes::Bytes;
use flate2::read::{GzDecoder, DeflateDecoder};

#[derive(Debug)]
pub struct UnsupportedCompression;
impl warp::reject::Reject for UnsupportedCompression {}

#[derive(Debug)]
pub struct DecompressionError;
impl warp::reject::Reject for DecompressionError {}

#[derive(Debug)]
pub struct NonUTF8Payload;
impl warp::reject::Reject for NonUTF8Payload {}

#[derive(Debug)]
pub struct InvalidJSONPayload;
impl warp::reject::Reject for InvalidJSONPayload {}

pub fn compressible_body() -> impl Filter<Extract = (String, ), Error = warp::Rejection> + Clone {
    warp::header::optional("content-encoding")
        .and(warp::body::bytes())
        .and_then(move |encoding: Option<String>, body: Bytes| async move {
            let bytes = body.to_vec();
            match encoding {
                Some(algorithm) => {
                    let mut uncompressed_body = String::new();
                    match algorithm.as_str() {
                        "gzip" => {
                            let mut decoder = GzDecoder::new(&bytes[..]);
                            decoder.read_to_string(&mut uncompressed_body)
                                .map_err(|_| warp::reject::custom(DecompressionError))
                                .map(|_| uncompressed_body)
                        },
                        "deflate" => {
                            let mut decoder = DeflateDecoder::new(&bytes[..]);
                            decoder.read_to_string(&mut uncompressed_body)
                                .map_err(|_| warp::reject::custom(DecompressionError))
                                .map(|_| uncompressed_body)
                        },
                        _ => Err(warp::reject::custom(UnsupportedCompression))
                    }
                },
                None => String::from_utf8(bytes).map_err(|_| warp::reject::custom(NonUTF8Payload))
            }
        })
}


#[derive(Debug)]
pub struct PayloadTooLarge;
impl warp::reject::Reject for PayloadTooLarge {}

pub fn content_length_filter(size_limit: ByteSize) -> impl Filter<Extract = (), Error = warp::Rejection> + Clone {
    warp::header("content-length").and_then(move |length: u128| async move {
        let bytes_max = size_limit.get_bytes();
        match bytes_max != 0 && length > bytes_max {
            true => Err(warp::reject::custom(PayloadTooLarge)),
            false => Ok(())
        }
    }).untuple_one()
}


#[derive(Debug)]
pub struct Unauthorized;
impl warp::reject::Reject for Unauthorized {}

#[derive(Debug)]
pub struct Forbidden;
impl warp::reject::Reject for Forbidden {}

pub fn validate_basic_auth(authorization_header: &str, expected_username: &str, expected_password: &str) -> bool {
    let header_split: Vec<&str> = authorization_header.split(' ').collect();
    if !header_split.get(0).map(|t| *t == "Basic").unwrap_or(false) {
        return false;
    }

    header_split.get(1)
        /* decode second part of header as base64 */
        .map(|b64_key| general_purpose::STANDARD.decode(b64_key).ok()).flatten().as_ref()
        /* re-encode into utf8 for string comparison */
        .map(|b64_decoded| std::str::from_utf8(b64_decoded).ok()).flatten()
        /* colon-separated, get first part (username) */
        .map(|utf8_encoded| utf8_encoded.split(':').collect::<Vec<&str>>())
        /* map to boolean: true if it matches, false otherwise */
        .map(|basic_split|
             basic_split.get(0).map(|submitted_username| submitted_username == &expected_username)
             .map(|username_ok| basic_split.get(1).map(|submitted_password| username_ok && submitted_password == &expected_password))
             .flatten()
        ).flatten()
        /* false if any of the above steps failed for any reason other than wrong creds */
        .unwrap_or(false)
}

pub fn write_key(write_keys_arc: Arc<HashSet<String>>) -> impl Filter<Extract = (String,), Error = warp::Rejection> + Clone {
    warp::header::optional("authorization").and_then(move |authorization: Option<String>| {
        let write_keys_arc_clone = write_keys_arc.clone();
        async move {
            match authorization {
                Some(submitted) => match write_keys_arc_clone.iter().find(|k| validate_basic_auth(&submitted, k, "")) {
                    Some(k) => Ok(k.clone()),
                    None => Err(warp::reject::custom(Forbidden))
                },
                None => Err(warp::reject::custom(Unauthorized))
            }
        }
    })
}

pub fn admin_auth_filter(admin: Arc<Option<Admin>>) -> impl Filter<Extract = (), Error = warp::Rejection> + Clone {
    warp::header::optional("authorization")
        .and(client_ip_filter())
        .and_then(move |authorization: Option<String>, client_ip: IpAddr| {
            let admin_arc_clone = admin.clone();
            let default_password = String::new();
            async move {
                let (admin_username, admin_password, admin_networks) = match admin_arc_clone.as_ref() {
                    Some(a) => (&a.username, &a.password, &a.allowed_networks),
                    None => (&None, &None, &None),
                };

                if let Some(expected_networks) = admin_networks {
                    if !expected_networks.iter().any(|n| n.contains(client_ip)) {
                        return Err(warp::reject::custom(Forbidden));
                    }
                }

                match admin_username {
                    Some(expected_username) => {
                        let expected_password = match admin_password {
                            Some(p) => p,
                            None => &default_password,
                        };
                        match authorization {
                            Some(submitted) => match validate_basic_auth(&submitted, &expected_username, &expected_password) {
                                true => Ok(()),
                                false => Err(warp::reject::custom(Forbidden))
                            },
                            None => Err(warp::reject::custom(Unauthorized))
                        }
                    },
                    None => Ok(())
                }
            }
        }
    ).untuple_one()
}


pub fn cors(origins: &Vec<String>) -> warp::cors::Builder {
    warp::cors()
        .allow_methods(&[Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers(["authorization", "anonymousid", "content-type"])
        .allow_credentials(true)
        .allow_origins(origins.iter().map(|s| s.as_str()))
}


pub struct BasicRequestInfo {
    pub client_ip: IpAddr,
    pub user_agent: Option<String>,
    pub request_id: Option<String>,
    pub method: String,
    pub path: String,
    pub length: String,
}

fn infer_client_ip(x_real_ip: Option<String>, x_forwarded_for: Option<String>, remote_addr: Option<SocketAddr>) -> IpAddr {
    let remote_addr = remote_addr.map(|addr| addr.ip().to_string());
    let client_ip = x_real_ip
        .or_else(|| x_forwarded_for.map(|forwarded_for| forwarded_for.split(",").next().map(|s| String::from(s))).flatten())
        .or(remote_addr);
    client_ip.map(|i| i.parse().expect("failed to parse client IP")).expect("failed to determine a client IP")
}

fn client_ip_filter() -> impl Filter<Extract = (IpAddr,), Error = warp::Rejection> + Clone {
    warp::header::optional("x-real-ip")
        .and(warp::header::optional("x-forwarded-for"))
        .and(warp::addr::remote())
        .map(|real_ip: Option<String>, forwarded_for: Option<String>, remote_addr: Option<SocketAddr>| {
            infer_client_ip(real_ip, forwarded_for, remote_addr)
        })
}

pub fn basic_request_info() -> impl Filter<Extract = (BasicRequestInfo,), Error = warp::Rejection> + Clone {
    client_ip_filter()
        .and(warp::header::optional("user-agent"))
        .and(warp::header::optional("x-request-id"))
        .and(warp::filters::method::method())
        .and(warp::filters::path::full())
        .and(warp::header::optional("content-length").map(|length: Option<String>| length.unwrap_or("0".into())))
        .map(|client_ip, user_agent, request_id, method: Method, path: FullPath, length: String| BasicRequestInfo {
            client_ip,
            user_agent,
            request_id,
            method: String::from(method.as_str()),
            path: String::from(path.as_str()),
            length,
        })
}


pub fn request_logger() -> impl Filter<Extract = (), Error = warp::Rejection> + Clone {
    warp::any().and(basic_request_info()).map(|info: BasicRequestInfo| {
        if info.path == "/" {
            /* Do not log the ping route, as this could get very verbose with active monitoring */
            return;
        }

        log::info!(
            "[request] [{}] {} {} from {} length {}",
            info.request_id.unwrap_or("?".into()),
            info.method,
            info.path,
            info.client_ip,
            info.length,
        );
    }).untuple_one()
}

pub fn response_logger(request_info: warp::log::Info) {
    if request_info.path() == "/" {
        /* Do not log the ping route, as this could get very verbose with active monitoring */
        return;
    }

    let headers = request_info.request_headers();
    let request_id = headers.get("x-request-id").map(|id| id.to_str().ok()).flatten().unwrap_or("?");
    let client_ip = infer_client_ip(
        headers.get("x-real-ip").map(|v| v.to_str().map(|s| String::from(s)).ok()).flatten(),
        headers.get("x-forwarded-for").map(|v| v.to_str().map(|s| String::from(s)).ok()).flatten(),
        request_info.remote_addr()
    );

    log::info!(
        "[response] [{}] {} {} from {} status {} duration {}ms",
        request_id,
        request_info.method(),
        request_info.path(),
        client_ip,
        request_info.status(),
        request_info.elapsed().as_millis()
    );
}


pub async fn handle_rejection(rejection: warp::Rejection) -> Result<impl warp::Reply, Infallible> {
    log::debug!("rejecting request: {:?}", rejection);

    if rejection.is_not_found() {
        Ok(Response::builder().status(warp::http::StatusCode::NOT_FOUND).body("KO"))
    } else if let Some(Unauthorized) = rejection.find() {
        Ok(Response::builder()
           .status(warp::http::StatusCode::UNAUTHORIZED)
           .header("WWW-Authenticate", "Basic realm=stilgar, charset=\"UTF-8\"")
           .body("KO"))
    } else if let Some(Forbidden) = rejection.find() {
        Ok(Response::builder().status(warp::http::StatusCode::FORBIDDEN).body("KO"))
    } else if let Some(PayloadTooLarge) = rejection.find() {
        Ok(Response::builder().status(warp::http::StatusCode::PAYLOAD_TOO_LARGE).body("KO"))
    } else if let Some(UnsupportedCompression) = rejection.find() {
        Ok(Response::builder().status(warp::http::StatusCode::UNSUPPORTED_MEDIA_TYPE).body("KO"))
    } else {
        Ok(Response::builder().status(warp::http::StatusCode::BAD_REQUEST).body("KO"))
    }
}
