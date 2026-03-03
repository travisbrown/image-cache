use crate::image_type::ImageType;
use hex::FromHex;
use imghdr::Type;
use md5::Digest;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Invalid file name")]
    InvalidFileName(PathBuf),
    #[error("Expected directory")]
    ExpectedDirectory(PathBuf),
    #[error("Unexpected digest")]
    UnexpectedDigest { expected: Digest, actual: Digest },
    #[error("Iteration error")]
    Iteration(#[from] IterationError),
}

#[derive(Debug, thiserror::Error)]
pub enum InitializationError {
    #[error("Invalid prefix part lengths")]
    InvalidPrefixPartLengths(Vec<usize>),
}

#[derive(Debug, thiserror::Error)]
pub enum IterationError {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Invalid file name")]
    InvalidFileName(PathBuf),
    #[error("Expected directory")]
    ExpectedDirectory(PathBuf),
    #[error("Expected file")]
    ExpectedFile(PathBuf),
    #[error("Hex parse error")]
    Hex(#[from] hex::FromHexError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Entry {
    pub path: PathBuf,
    pub digest: Digest,
}

impl Entry {
    pub fn validate(&self) -> Result<Result<(), Digest>, std::io::Error> {
        let bytes = std::fs::read(&self.path)?;
        let digest = md5::compute(&bytes);

        if digest == self.digest {
            Ok(Ok(()))
        } else {
            Ok(Err(digest))
        }
    }
}

#[derive(Clone, Debug)]
pub struct PrefixPartLengths(pub Vec<usize>);

impl std::str::FromStr for PrefixPartLengths {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.split('/')
            .map(str::parse)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| s.to_string())
            .map(PrefixPartLengths)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValidationResult {
    Valid { entry: Entry },
    Invalid { entry: Entry, actual: Digest },
}

impl ValidationResult {
    pub fn result(self) -> Result<Entry, Error> {
        match self {
            Self::Valid { entry } => Ok(entry),
            Self::Invalid { entry, actual } => Err(Error::UnexpectedDigest {
                expected: entry.digest,
                actual,
            }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Action {
    pub entry: Entry,
    pub image_type: ImageType,
    pub added: bool,
}

impl Action {
    #[must_use]
    pub const fn image_type(&self) -> Option<Type> {
        self.image_type.value()
    }
}

#[derive(Clone)]
pub struct Store {
    pub base: PathBuf,
    pub prefix_part_lengths: Vec<usize>,
}

impl Store {
    pub fn new<P: AsRef<Path>>(base: P) -> Self {
        Self {
            base: base.as_ref().to_path_buf(),
            prefix_part_lengths: vec![],
        }
    }

    pub fn with_prefix_part_lengths<T: AsRef<[usize]>>(
        self,
        prefix_part_lengths: T,
    ) -> Result<Self, InitializationError> {
        if prefix_part_lengths.as_ref().iter().copied().sum::<usize>() > 32
            || prefix_part_lengths.as_ref().contains(&0)
        {
            Err(InitializationError::InvalidPrefixPartLengths(
                prefix_part_lengths.as_ref().to_vec(),
            ))
        } else {
            Ok(Self {
                base: self.base,
                prefix_part_lengths: prefix_part_lengths.as_ref().to_vec(),
            })
        }
    }

    /// Infer the prefix part lengths used to create a store.
    ///
    /// The result will be empty if and only if the store has no files (even if there are directories).
    ///
    /// If this function returns a result, it is guaranteed to be correct if the store is valid, but the validity is not checked.
    pub fn infer_prefix_part_lengths<P: AsRef<Path>>(base: P) -> Result<Option<Vec<usize>>, Error> {
        if base.as_ref().is_dir() {
            let first = std::fs::read_dir(base)?
                .next()
                .map_or(Ok(None), |entry| entry.map(|entry| Some(entry.path())))?;

            let mut acc = vec![];

            let is_empty = first
                .map(|first| Self::infer_prefix_part_lengths_rec(&first, &mut acc))
                .map_or(Ok(true), |value| value)?;

            Ok(if is_empty { None } else { Some(acc) })
        } else {
            Err(Error::ExpectedDirectory(base.as_ref().to_path_buf()))
        }
    }

    // Return value indicates whether the store has no files.
    fn infer_prefix_part_lengths_rec<P: AsRef<Path>>(
        current: P,
        acc: &mut Vec<usize>,
    ) -> Result<bool, Error> {
        if current.as_ref().is_file() {
            Ok(false)
        } else {
            let file_name = current
                .as_ref()
                .file_name()
                .ok_or_else(|| Error::InvalidFileName(current.as_ref().to_path_buf()))?;

            acc.push(file_name.len());

            let next = std::fs::read_dir(current)?
                .next()
                .map_or(Ok(None), |entry| entry.map(|entry| Some(entry.path())))?;

            next.map_or(Ok(true), |next| {
                Self::infer_prefix_part_lengths_rec(next, acc)
            })
        }
    }

    #[must_use]
    pub fn entries(&self) -> Entries<'_> {
        Entries {
            stack: vec![vec![self.base.clone()]],
            level: None,
            prefix_part_lengths: &self.prefix_part_lengths,
        }
    }

    pub fn save<T: AsRef<[u8]> + Copy>(&self, bytes: T) -> Result<Action, Error> {
        // The image type check will fail with an error if there aren't enough bytes.
        let image_type = if bytes.as_ref().len() < 8 {
            None
        } else {
            imghdr::from_bytes(bytes.as_ref())
        };

        let digest = md5::compute(bytes);
        let path = self.path(digest);

        // We construct the path, so we know there will always be a parent.
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let added = if path.exists() {
            false
        } else {
            let mut file = File::create(&path)?;
            file.write_all(bytes.as_ref())?;

            true
        };

        Ok(Action {
            entry: Entry { path, digest },
            image_type: ImageType::new(image_type),
            added,
        })
    }

    #[must_use]
    pub fn path(&self, digest: Digest) -> PathBuf {
        let digest_string = format!("{digest:x}");
        let mut digest_remaining = digest_string.as_str();
        let mut path = self.base.clone();

        for prefix_part_length in &self.prefix_part_lengths {
            let next = &digest_remaining[0..*prefix_part_length];
            digest_remaining = &digest_remaining[*prefix_part_length..];

            path.push(next);
        }

        path.push(digest_string);

        path
    }
}

pub struct Entries<'a> {
    stack: Vec<Vec<PathBuf>>,
    level: Option<usize>,
    prefix_part_lengths: &'a [usize],
}

impl Entries<'_> {
    fn is_last(&self) -> bool {
        self.level == Some(self.prefix_part_lengths.len())
    }

    fn current_prefix_part_length(&self) -> Option<usize> {
        self.level
            .and_then(|level| self.prefix_part_lengths.get(level))
            .copied()
    }

    fn increment_level(&mut self) {
        self.level = Some(self.level.take().map_or(0, |level| level + 1));
    }

    const fn decrement_level(&mut self) {
        if let Some(level) = self.level.take()
            && level != 0
        {
            self.level = Some(level - 1);
        }
    }

    const fn is_valid_char(byte: u8) -> bool {
        byte.is_ascii_lowercase() || byte.is_ascii_digit()
    }

    fn path_to_entry(path: PathBuf) -> Result<Entry, IterationError> {
        if path.is_file() {
            path.file_name()
                .ok_or_else(|| IterationError::InvalidFileName(path.clone()))
                .and_then(|file_name| {
                    let file_name_bytes = file_name.as_encoded_bytes();

                    if file_name_bytes
                        .iter()
                        .all(|byte| Self::is_valid_char(*byte))
                    {
                        <[u8; 16]>::from_hex(file_name_bytes).map_err(IterationError::from)
                    } else {
                        Err(IterationError::InvalidFileName(path.clone()))
                    }
                })
                .map(Digest)
                .map(|digest| Entry { path, digest })
        } else {
            Err(IterationError::ExpectedFile(path))
        }
    }

    fn path_to_paths(
        path: PathBuf,
        prefix_part_length: Option<usize>,
    ) -> Result<Vec<PathBuf>, IterationError> {
        if path.is_dir() {
            let mut paths = std::fs::read_dir(path)?
                .map(|entry| entry.map(|entry| entry.path()))
                .collect::<Result<Vec<PathBuf>, std::io::Error>>()
                .map_err(IterationError::from)?;

            paths.sort_unstable_by(|first, second| second.cmp(first));

            match prefix_part_length {
                Some(prefix_part_length) => {
                    let invalid_path = paths.iter().find(|path| {
                        path.file_name().is_none_or(|file_name| {
                            file_name.len() != prefix_part_length
                                && file_name
                                    .as_encoded_bytes()
                                    .iter()
                                    .any(|byte| !Self::is_valid_char(*byte))
                        })
                    });

                    // Clippy is wrong here.
                    #[allow(clippy::option_if_let_else)]
                    match invalid_path {
                        Some(invalid_path) => {
                            Err(IterationError::InvalidFileName(invalid_path.clone()))
                        }
                        None => Ok(paths),
                    }
                }
                None => Ok(paths),
            }
        } else {
            Err(IterationError::ExpectedDirectory(path))
        }
    }

    pub fn validate(self) -> impl Iterator<Item = Result<ValidationResult, IterationError>> {
        self.map(|entry| {
            let entry = entry?;

            Ok(match entry.validate()? {
                Ok(()) => ValidationResult::Valid { entry },
                Err(actual) => ValidationResult::Invalid { entry, actual },
            })
        })
    }

    pub fn validate_fail_fast(self) -> impl Iterator<Item = Result<Entry, Error>> {
        self.validate().map(|result| {
            result
                .map_err(Error::from)
                .and_then(ValidationResult::result)
        })
    }
}

impl Iterator for Entries<'_> {
    type Item = Result<Entry, IterationError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.stack.pop().and_then(|mut next_paths| {
            if self.is_last() {
                if let Some(next_path) = next_paths.pop() {
                    self.stack.push(next_paths);

                    Some(Self::path_to_entry(next_path))
                } else {
                    self.decrement_level();

                    self.next()
                }
            } else if let Some(next_path) = next_paths.pop() {
                Self::path_to_paths(next_path, self.current_prefix_part_length()).map_or_else(
                    |error| Some(Err(error)),
                    |next_level| {
                        self.stack.push(next_paths);
                        self.stack.push(next_level);
                        self.increment_level();

                        self.next()
                    },
                )
            } else {
                self.decrement_level();

                self.next()
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use hex::FromHex;

    const MINIMAL_JPG_HEX: &str = "ffd8ffe000104a46494600010100000100010000ffdb004300080606070605080707070909080a0c140d0c0b0b0c1912130f141d1a1f1e1d1a1c1c20242e2720222c231c1c2837292c30313434341f27393d38323c2e333432ffdb0043010909090c0b0c180d0d1832211c21323232323232323232323232323232323232323232323232323232323232323232323232323232ffc00011080001000103011100021101031101ffc4001f00000105010101010101000000000000000102030405060708090a0bffc400b51000020103030204030505040400017d010203000411051221314106135161712232819114a1b1c1d1f0e123f1ffda000c03010002110311003f00ff00ffd9";
    const MINIMAL_PNG_HEX: &str = "89504e470d0a1a0a0000000d4948445200000001000000010802000000907724d90000000a49444154789c6360000002000185d114090000000049454e44ae426082";

    fn minimal_jpg_bytes() -> Vec<u8> {
        hex::decode(MINIMAL_JPG_HEX).unwrap()
    }

    fn minimal_png_bytes() -> Vec<u8> {
        hex::decode(MINIMAL_PNG_HEX).unwrap()
    }

    fn empty_bytes() -> Vec<u8> {
        vec![]
    }

    fn text_bytes() -> Vec<u8> {
        "foo bar baz".as_bytes().to_vec()
    }

    fn minimal_jpg_digest() -> [u8; 16] {
        FromHex::from_hex("79c09c11a8f92599f3c6d389564dd24d").unwrap()
    }

    fn minimal_png_digest() -> [u8; 16] {
        FromHex::from_hex("ddf93a3305d41f70e19bb8a04ac673a5").unwrap()
    }

    fn empty_digest() -> [u8; 16] {
        FromHex::from_hex("d41d8cd98f00b204e9800998ecf8427e").unwrap()
    }

    fn text_digest() -> [u8; 16] {
        FromHex::from_hex("ab07acbb1e496801937adfa772424bf7").unwrap()
    }

    fn test_save(
        prefix_part_lengths: Vec<usize>,
    ) -> Result<Vec<super::Entry>, Box<dyn std::error::Error>> {
        let base = tempfile::tempdir()?;

        let store = super::Store::new(base.path().to_path_buf())
            .with_prefix_part_lengths(&prefix_part_lengths)?;
        let minimal_jpg_action = store.save(&minimal_jpg_bytes())?;
        let minimal_png_action = store.save(&minimal_png_bytes())?;
        let empty_action = store.save(&empty_bytes())?;
        let text_action = store.save(&text_bytes())?;

        assert!(minimal_jpg_action.added);
        assert!(minimal_png_action.added);
        assert!(empty_action.added);
        assert!(text_action.added);

        assert_eq!(minimal_jpg_action.image_type(), Some(imghdr::Type::Jpeg));
        assert_eq!(minimal_png_action.image_type(), Some(imghdr::Type::Png));
        assert_eq!(empty_action.image_type(), None);
        assert_eq!(text_action.image_type(), None);

        let repeat_minimal_jpg_action = store.save(&minimal_jpg_bytes())?;
        let repeat_minimal_png_action = store.save(&minimal_png_bytes())?;
        let repeat_empty_action = store.save(&empty_bytes())?;
        let repeat_text_action = store.save(&text_bytes())?;

        assert!(!repeat_minimal_jpg_action.added);
        assert!(!repeat_minimal_png_action.added);
        assert!(!repeat_empty_action.added);
        assert!(!repeat_text_action.added);

        let inferred_prefix_parts_length = super::Store::infer_prefix_part_lengths(base.path())?;

        assert_eq!(inferred_prefix_parts_length, Some(prefix_part_lengths));

        let entries = store.entries().collect::<Result<Vec<_>, _>>()?;
        let digests = entries
            .iter()
            .map(|entry| entry.digest.0)
            .collect::<Vec<_>>();

        let expected_digests = vec![
            minimal_jpg_digest(),
            text_digest(),
            empty_digest(),
            minimal_png_digest(),
        ];

        assert_eq!(entries.len(), 4);
        assert_eq!(digests, expected_digests);

        Ok(entries)
    }

    #[test]
    fn test_save_empty() -> Result<(), Box<dyn std::error::Error>> {
        test_save(vec![])?;

        Ok(())
    }

    #[test]
    fn test_save_1() -> Result<(), Box<dyn std::error::Error>> {
        test_save(vec![1])?;

        Ok(())
    }

    #[test]
    fn test_save_2_2() -> Result<(), Box<dyn std::error::Error>> {
        test_save(vec![2, 2])?;

        Ok(())
    }

    #[test]
    fn test_save_16_3() -> Result<(), Box<dyn std::error::Error>> {
        test_save(vec![16, 3])?;

        Ok(())
    }

    #[test]
    fn test_save_19_13() -> Result<(), Box<dyn std::error::Error>> {
        test_save(vec![19, 13])?;

        Ok(())
    }
}
