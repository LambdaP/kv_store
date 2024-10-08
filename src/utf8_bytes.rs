use bytes::Bytes;
use std::ops::Deref;
use std::str::Utf8Error;

#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct Utf8Bytes(Bytes);

impl Utf8Bytes {
    pub const fn empty() -> Self {
        Self::from_static("")
    }
    pub const fn from_static(s: &'static str) -> Self {
        Self(Bytes::from_static(s.as_bytes()))
    }

    pub const unsafe fn from_bytes_unchecked(bytes: Bytes) -> Self {
        Self(bytes)
    }

    pub fn copy_from_slice(s: &str) -> Self {
        Self(Bytes::copy_from_slice(s.as_bytes()))
    }
}

impl From<&'static str> for Utf8Bytes {
    fn from(s: &'static str) -> Utf8Bytes {
        Self::from_static(s)
    }
}

impl TryFrom<&'static [u8]> for Utf8Bytes {
    type Error = Utf8Error;

    fn try_from(bytes: &'static [u8]) -> Result<Self, Self::Error> {
        std::str::from_utf8(bytes).map(Self::from_static)
    }
}

impl From<Box<str>> for Utf8Bytes {
    fn from(s: Box<str>) -> Utf8Bytes {
        // TODO this might be slightly suboptimal
        Self(Bytes::from(String::from(s)))
    }
}

impl TryFrom<Box<[u8]>> for Utf8Bytes {
    type Error = Utf8Error;

    fn try_from(bytes: Box<[u8]>) -> Result<Self, Self::Error> {
        _ = std::str::from_utf8(&bytes)?;

        Ok(Self(Bytes::from(bytes)))
    }
}

impl From<String> for Utf8Bytes {
    fn from(s: String) -> Utf8Bytes {
        Self(Bytes::from(s))
    }
}

impl TryFrom<Bytes> for Utf8Bytes {
    type Error = Utf8Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        _ = std::str::from_utf8(&bytes)?;

        Ok(Self(bytes))
    }
}

impl Deref for Utf8Bytes {
    type Target = str;

    fn deref(&self) -> &str {
        // Safe because UTF-8 was validated at construction
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }
}

impl AsRef<Bytes> for Utf8Bytes {
    fn as_ref(&self) -> &Bytes {
        &self.0
    }
}

impl AsRef<[u8]> for Utf8Bytes {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<str> for Utf8Bytes {
    fn as_ref(&self) -> &str {
        self
    }
}

impl From<Utf8Bytes> for Bytes {
    fn from(utf8_bytes: Utf8Bytes) -> Bytes {
        utf8_bytes.0
    }
}

impl PartialEq<str> for Utf8Bytes {
    fn eq(&self, other: &str) -> bool {
        **self == *other
    }
}

impl<'a> PartialEq<&'a str> for Utf8Bytes {
    fn eq(&self, other: &&'a str) -> bool {
        **self == **other
    }
}

impl PartialEq<Bytes> for Utf8Bytes {
    fn eq(&self, other: &Bytes) -> bool {
        self.0 == *other
    }
}

impl PartialEq<[u8]> for Utf8Bytes {
    fn eq(&self, other: &[u8]) -> bool {
        self.0 == *other
    }
}

impl<'a> PartialEq<&'a [u8]> for Utf8Bytes {
    fn eq(&self, other: &&'a [u8]) -> bool {
        self.0 == *other
    }
}

mod serde {
    use super::Utf8Bytes;
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    impl<'de> Deserialize<'de> for Utf8Bytes {
        fn deserialize<D>(deserializer: D) -> Result<Utf8Bytes, D::Error>
        where
            D: Deserializer<'de>,
        {
            use serde::de::Error;
            Bytes::deserialize(deserializer)?
                .try_into()
                .map_err(Error::custom)
        }
    }

    impl Serialize for Utf8Bytes {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.serialize_str(self)
        }
    }
}

mod axum {
    use super::Utf8Bytes;
    use axum::{async_trait, extract::Request, http::StatusCode, response::IntoResponse};
    use bytes::Bytes;

    #[async_trait]
    impl<S> axum::extract::FromRequest<S> for Utf8Bytes
    where
        S: Send + Sync,
    {
        type Rejection = (StatusCode, String);

        #[tracing::instrument(level = "trace", skip(req, state))]
        async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
            let body = Bytes::from_request(req, state)
                .await
                .map_err(|err| (err.status(), err.body_text()))?;

            body.try_into().map_err(|_| {
                (
                    StatusCode::BAD_REQUEST,
                    "Request body didn't contain valid UTF-8".into(),
                )
            })
        }
    }

    impl IntoResponse for Utf8Bytes {
        #[tracing::instrument(level = "trace", skip(self))]
        fn into_response(self) -> axum::response::Response {
            // String::from(&*self).into_response()
            use axum::{body::Body, http::header};

            const TEXT_PLAIN_UTF_8: &str = "text/plain; charset=utf-8";
            const HEADER_VALUE: header::HeaderValue =
                header::HeaderValue::from_static(TEXT_PLAIN_UTF_8);

            let mut res = Body::from(Bytes::from(self)).into_response();
            res.headers_mut().insert(header::CONTENT_TYPE, HEADER_VALUE);
            res
        }
    }
}
