use crate::events::any::EventOrBatch;

use warp;
use warp::Filter;
use warp::http::Method;
use byte_unit::Byte as ByteSize;
use base64::{Engine as _, engine::general_purpose};
use std::convert::Infallible;
use std::io::prelude::*;
use std::sync::Arc;
use bytes::Bytes;
use serde_json;
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

pub fn compressible_event() -> impl Filter<Extract = (EventOrBatch, ), Error = warp::Rejection> + Clone {
    warp::header::optional("content-encoding")
        .and(warp::body::bytes())
        .and_then(move |encoding: Option<String>, body: Bytes| async move {
            let bytes = body.to_vec();

            let body_str = match encoding {
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
            }?;

            serde_json::from_str(&body_str).map_err(|_| warp::reject::custom(InvalidJSONPayload))
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

fn validate_basic_auth(expected_key: &String, authorization_header: &String) -> bool {
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
        .map(|basic_split| basic_split.get(0).map(|submitted_key| submitted_key == expected_key)).flatten()
        /* false if any of the above steps failed for any reason other than wrong creds */
        .unwrap_or(false)
}

pub fn auth_filter(write_key_arc: Arc<Option<String>>) -> impl Filter<Extract = (), Error = warp::Rejection> + Clone {
    warp::header::optional("authorization").and_then(move |authorization: Option<String>| {
        let write_key_arc_clone = write_key_arc.clone();
        async move {
            match write_key_arc_clone.as_ref() {
                Some(expected) => match authorization {
                    Some(submitted) => match validate_basic_auth(expected, &submitted) {
                        true => Ok(()),
                        false => Err(warp::reject::custom(Forbidden))
                    },
                    None => Err(warp::reject::custom(Unauthorized))
                },
                None => Ok(())
            }
        }
    }).untuple_one()
}


pub fn cors(origins: &Vec<String>) -> warp::cors::Builder {
    warp::cors()
        .allow_methods(&[Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers(["authorization", "anonymousid", "content-type"])
        .allow_credentials(true)
        .allow_origins(origins.iter().map(|s| s.as_str()))
}


pub async fn handle_rejection(rejection: warp::Rejection) -> Result<impl warp::Reply, Infallible> {
    log::debug!("rejecting request: {:?}", rejection);

    if rejection.is_not_found() {
        Ok(warp::reply::with_status("KO", warp::http::StatusCode::NOT_FOUND))
    } else if let Some(Unauthorized) = rejection.find() {
        Ok(warp::reply::with_status("KO", warp::http::StatusCode::UNAUTHORIZED))
    } else if let Some(Forbidden) = rejection.find() {
        Ok(warp::reply::with_status("KO", warp::http::StatusCode::FORBIDDEN))
    } else if let Some(PayloadTooLarge) = rejection.find() {
        Ok(warp::reply::with_status("KO", warp::http::StatusCode::PAYLOAD_TOO_LARGE))
    } else if let Some(UnsupportedCompression) = rejection.find() {
        Ok(warp::reply::with_status("KO", warp::http::StatusCode::UNSUPPORTED_MEDIA_TYPE))
    } else {
        Ok(warp::reply::with_status("KO", warp::http::StatusCode::BAD_REQUEST))
    }
}
