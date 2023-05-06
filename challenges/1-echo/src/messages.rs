use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub(crate) enum InboundRequest {
    Echo { echo: String },
}

impl InboundRequest {
    pub(crate) fn echo(self) -> String {
        match self {
            InboundRequest::Echo { echo } => echo,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub(crate) enum OutboundResponse {
    EchoOk { echo: String },
}
