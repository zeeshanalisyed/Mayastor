use crate::{BdevClient, JsonClient, MayaClient};
use byte_unit::Byte;
use bytes::Bytes;
use clap::ArgMatches;
use http::uri::{Authority, PathAndQuery, Scheme, Uri};
use snafu::{Backtrace, ResultExt, Snafu};
use std::{cmp::max, str::FromStr};
use tonic::transport::Endpoint;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid URI"))]
    InvalidUriBytes {
        source: http::uri::InvalidUri,
        backtrace: Backtrace,
    },
    #[snafu(display("Invalid URI parts"))]
    InvalidUriParts {
        source: http::uri::InvalidUriParts,
        backtrace: Backtrace,
    },
    #[snafu(display("Invalid URI"))]
    TonicInvalidUri {
        source: tonic::codegen::http::uri::InvalidUri,
        backtrace: Backtrace,
    },
    #[snafu(display("Invalid URI"))]
    InvalidUri {
        source: http::uri::InvalidUri,
        backtrace: Backtrace,
    },
    #[snafu(display("Invalid output format: {}", format))]
    OutputFormatError { format: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OutputFormat {
    Json,
    Default,
}

impl FromStr for OutputFormat {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(Self::Json),
            "default" => Ok(Self::Default),
            s => Err(Error::OutputFormatError {
                format: s.to_string(),
            }),
        }
    }
}

pub struct Context {
    pub(crate) client: MayaClient,
    pub(crate) bdev: BdevClient,
    pub(crate) json: JsonClient,
    verbosity: u64,
    units: char,
    pub(crate) output: OutputFormat,
}

impl Context {
    pub(crate) async fn new(matches: &ArgMatches<'_>) -> Result<Self, Error> {
        let verbosity = if matches.is_present("quiet") {
            0
        } else {
            matches.occurrences_of("verbose") + 1
        };
        let units = matches
            .value_of("units")
            .and_then(|u| u.chars().next())
            .unwrap_or('b');
        // Ensure the provided host is defaulted & normalized to what we expect.
        let host = if let Some(host) = matches.value_of("bind") {
            let uri = host.parse::<Uri>().context(InvalidUri)?;
            let mut parts = uri.into_parts();
            if parts.scheme.is_none() {
                parts.scheme = Scheme::from_str("http").ok();
            }
            if let Some(ref mut authority) = parts.authority {
                if authority.port().is_none() {
                    parts.authority = Authority::from_maybe_shared(Bytes::from(
                        format!("{}:{}", authority.host(), 10124),
                    ))
                    .ok()
                }
            }
            if parts.path_and_query.is_none() {
                parts.path_and_query = PathAndQuery::from_str("/").ok();
            }
            let uri = Uri::from_parts(parts).context(InvalidUriParts)?;
            Endpoint::from(uri)
        } else {
            Endpoint::from_static("http://127.0.0.1:10124")
        };

        if verbosity > 1 {
            println!("Connecting to {:?}", host);
        }

        let output = matches.value_of("output").ok_or_else(|| {
            Error::OutputFormatError {
                format: "<none>".to_string(),
            }
        })?;
        let output = output.parse()?;

        let client = MayaClient::connect(host.clone()).await.unwrap();
        let bdev = BdevClient::connect(host.clone()).await.unwrap();
        let json = JsonClient::connect(host).await.unwrap();

        Ok(Context {
            client,
            bdev,
            json,
            verbosity,
            units,
            output,
        })
    }
    pub(crate) fn v1(&self, s: &str) {
        if self.verbosity > 0 {
            println!("{}", s)
        }
    }

    pub(crate) fn v2(&self, s: &str) {
        if self.verbosity > 1 {
            println!("{}", s)
        }
    }

    pub(crate) fn units(&self, n: Byte) -> String {
        match self.units {
            'i' => n.get_appropriate_unit(true).to_string(),
            'd' => n.get_appropriate_unit(false).to_string(),
            _ => n.get_bytes().to_string(),
        }
    }

    pub(crate) fn print_list(
        &self,
        headers: Vec<&str>,
        mut data: Vec<Vec<String>>,
    ) {
        assert_ne!(data.len(), 0);
        let ncols = data.first().unwrap().len();
        assert_eq!(headers.len(), ncols);

        let columns = if self.verbosity > 0 {
            data.insert(
                0,
                headers
                    .iter()
                    .map(|h| {
                        if let Some(stripped) = h.strip_prefix('>') {
                            stripped.to_string()
                        } else {
                            h.to_string()
                        }
                    })
                    .collect(),
            );

            data.iter().fold(
                headers
                    .iter()
                    .map(|h| (h.starts_with('>'), 0usize))
                    .collect(),
                |thus_far: Vec<(bool, usize)>, elem| {
                    thus_far
                        .iter()
                        .zip(elem)
                        .map(|((a, l), s)| (*a, max(*l, s.len())))
                        .collect()
                },
            )
        } else {
            vec![(false, 0usize); ncols]
        };

        for row in data {
            let vals = row.iter().enumerate().map(|(idx, s)| {
                if columns[idx].0 {
                    format!("{:>1$}", s, columns[idx].1)
                } else {
                    format!("{:<1$}", s, columns[idx].1)
                }
            });

            println!("{}", vals.collect::<Vec<String>>().join(" "));
        }
    }
}
