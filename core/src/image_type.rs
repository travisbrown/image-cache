use imghdr::Type;
use std::borrow::Cow;
use std::fmt::Display;
use std::str::FromStr;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ImageType(Option<Type>);

impl ImageType {
    #[must_use]
    pub const fn new(image_type: Option<Type>) -> Self {
        Self(image_type)
    }

    #[must_use]
    pub const fn empty() -> Self {
        Self(None)
    }

    #[must_use]
    pub const fn value(self) -> Option<Type> {
        self.0
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self.0 {
            None => "",
            Some(Type::Bgp) => "bgp",
            Some(Type::Bmp) => "bmp",
            Some(Type::Exr) => "exr",
            Some(Type::Flif) => "flif",
            Some(Type::Gif) => "gif",
            Some(Type::Ico) => "ico",
            Some(Type::Jpeg) => "jpeg",
            Some(Type::Pbm) => "pbm",
            Some(Type::Pgm) => "pgm",
            Some(Type::Png) => "png",
            Some(Type::Ppm) => "ppm",
            Some(Type::Rast) => "rast",
            Some(Type::Rgb) => "rgb",
            Some(Type::Rgbe) => "rgbe",
            Some(Type::Tiff) => "tiff",
            Some(Type::Webp) => "webp",
            Some(Type::Xbm) => "xbm",
        }
    }

    #[must_use]
    pub fn mime_type(self) -> Option<mime::Mime> {
        self.0.and_then(|image_type| match image_type {
            Type::Bmp => Some(mime::IMAGE_BMP),
            Type::Gif => Some(mime::IMAGE_GIF),
            Type::Ico => "image/x-icon".parse().ok(),
            Type::Jpeg => Some(mime::IMAGE_JPEG),
            Type::Png => Some(mime::IMAGE_PNG),
            Type::Tiff => "image/tiff".parse().ok(),
            Type::Webp => "image/webp".parse().ok(),
            _ => None,
        })
    }

    #[must_use]
    pub const fn code(self) -> u8 {
        match self.0 {
            None => 0,
            Some(Type::Bgp) => 1,
            Some(Type::Bmp) => 2,
            Some(Type::Exr) => 3,
            Some(Type::Flif) => 4,
            Some(Type::Gif) => 5,
            Some(Type::Ico) => 6,
            Some(Type::Jpeg) => 7,
            Some(Type::Pbm) => 8,
            Some(Type::Pgm) => 9,
            Some(Type::Png) => 10,
            Some(Type::Ppm) => 11,
            Some(Type::Rast) => 12,
            Some(Type::Rgb) => 13,
            Some(Type::Rgbe) => 14,
            Some(Type::Tiff) => 15,
            Some(Type::Webp) => 16,
            Some(Type::Xbm) => 17,
        }
    }

    #[must_use]
    pub const fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self(None)),
            1 => Some(Self(Some(Type::Bgp))),
            2 => Some(Self(Some(Type::Bmp))),
            3 => Some(Self(Some(Type::Exr))),
            4 => Some(Self(Some(Type::Flif))),
            5 => Some(Self(Some(Type::Gif))),
            6 => Some(Self(Some(Type::Ico))),
            7 => Some(Self(Some(Type::Jpeg))),
            8 => Some(Self(Some(Type::Pbm))),
            9 => Some(Self(Some(Type::Pgm))),
            10 => Some(Self(Some(Type::Png))),
            11 => Some(Self(Some(Type::Ppm))),
            12 => Some(Self(Some(Type::Rast))),
            13 => Some(Self(Some(Type::Rgb))),
            14 => Some(Self(Some(Type::Rgbe))),
            15 => Some(Self(Some(Type::Tiff))),
            16 => Some(Self(Some(Type::Webp))),
            17 => Some(Self(Some(Type::Xbm))),
            _ => None,
        }
    }
}

impl From<Type> for ImageType {
    fn from(value: Type) -> Self {
        Self(Some(value))
    }
}

impl FromStr for ImageType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "" => Ok(Self(None)),
            "bgp" => Ok(Self(Some(Type::Bgp))),
            "bmp" => Ok(Self(Some(Type::Bmp))),
            "exr" => Ok(Self(Some(Type::Exr))),
            "flif" => Ok(Self(Some(Type::Flif))),
            "gif" => Ok(Self(Some(Type::Gif))),
            "ico" => Ok(Self(Some(Type::Ico))),
            "jpeg" => Ok(Self(Some(Type::Jpeg))),
            "pbm" => Ok(Self(Some(Type::Pbm))),
            "pgm" => Ok(Self(Some(Type::Pgm))),
            "png" => Ok(Self(Some(Type::Png))),
            "ppm" => Ok(Self(Some(Type::Ppm))),
            "rast" => Ok(Self(Some(Type::Rast))),
            "rgb" => Ok(Self(Some(Type::Rgb))),
            "rgbe" => Ok(Self(Some(Type::Rgbe))),
            "tiff" => Ok(Self(Some(Type::Tiff))),
            "webp" => Ok(Self(Some(Type::Webp))),
            "xbm" => Ok(Self(Some(Type::Xbm))),
            other => Err(other.to_string()),
        }
    }
}

impl Display for ImageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<'de> serde::de::Deserialize<'de> for ImageType {
    fn deserialize<D: serde::de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let as_str: Cow<'de, str> = serde::de::Deserialize::deserialize(deserializer)?;

        as_str.parse::<Self>().map_err(|_| {
            serde::de::Error::invalid_value(
                serde::de::Unexpected::Str(&as_str),
                &"a lowercase image extension",
            )
        })
    }
}

impl serde::ser::Serialize for ImageType {
    fn serialize<S: serde::ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.as_str().serialize(serializer)
    }
}

impl<C> bincode::de::Decode<C> for ImageType {
    fn decode<D: bincode::de::Decoder<Context = C>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let code = u8::decode(decoder)?;

        Self::from_code(code).ok_or(bincode::error::DecodeError::Other(
            "invalid image type code",
        ))
    }
}

impl<'de, C> bincode::de::BorrowDecode<'de, C> for ImageType {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de, Context = C>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        bincode::Decode::decode(decoder)
    }
}

impl bincode::enc::Encode for ImageType {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        bincode::enc::Encode::encode(&self.code(), encoder)
    }
}
