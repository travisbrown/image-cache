use crate::image_type::ImageType;
use imghdr::Type;
use md5::Digest;
use prefix_file_tree::{
    Entry, Tree,
    scheme::{Case, Identity, hex::Hex},
};
use std::path::{Path, PathBuf};

/// Alias for the concrete tree type: lowercase MD5 hex names, no file extension.
type HexTree = Tree<Hex<16>>;

/// The scheme instance shared by all `Store` constructors.
const SCHEME: Hex<16> = Hex { case: Case::Lower };

#[derive(Debug, thiserror::Error)]
pub enum InitializationError {
    #[error("Invalid prefix part lengths")]
    InvalidPrefixPartLengths(Vec<usize>),
}

#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error(transparent)]
    Iteration(#[from] prefix_file_tree::iter::Error),
    #[error("Unexpected digest")]
    UnexpectedDigest { expected: Digest, actual: Digest },
}

/// Parsed form of the `--prefix` CLI argument (e.g. `"2/2"` is parsed as `[2, 2]`).
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
    Valid {
        entry: Entry<[u8; 16]>,
    },
    Invalid {
        entry: Entry<[u8; 16]>,
        actual: Digest,
    },
}
impl ValidationResult {
    pub fn result(self) -> Result<Entry<[u8; 16]>, ValidationError> {
        match self {
            Self::Valid { entry } => Ok(entry),
            Self::Invalid { entry, actual } => Err(ValidationError::UnexpectedDigest {
                expected: Digest(entry.name),
                actual,
            }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Action {
    pub entry: Entry<[u8; 16]>,
    pub image_type: ImageType,
    pub added: bool,
}

impl Action {
    /// Return the MD5 digest of the stored content.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        Digest(self.entry.name)
    }

    #[must_use]
    pub const fn image_type(&self) -> Option<Type> {
        self.image_type.value()
    }
}

#[derive(Clone)]
pub struct Store {
    base: PathBuf,
    tree: HexTree,
}

impl Store {
    /// Create a store rooted at `base` with no directory prefix.
    pub fn new<P: AsRef<Path>>(base: P) -> Self {
        let base = base.as_ref().to_path_buf();

        #[allow(clippy::missing_panics_doc)]
        // An unconstrained `Hex<16>` tree always builds successfully.
        let tree = Tree::builder(&base)
            .with_scheme(SCHEME)
            .with_no_extension()
            .build()
            .expect("Tree builder failed (should never happen)");

        Self { base, tree }
    }

    /// Configure the prefix lengths (e.g. `[2, 2]` → `ab/cd/<digest>`).
    ///
    /// # Errors
    ///
    /// Returns [`InitializationError::InvalidPrefixPartLengths`] if the lengths
    /// sum to more than 32 (the full MD5 hex width) or if any length is zero.
    pub fn with_prefix_part_lengths<T: AsRef<[usize]>>(
        self,
        prefix_part_lengths: T,
    ) -> Result<Self, InitializationError> {
        let lengths = prefix_part_lengths.as_ref();

        if lengths.iter().copied().sum::<usize>() > 32 || lengths.contains(&0) {
            return Err(InitializationError::InvalidPrefixPartLengths(
                lengths.to_vec(),
            ));
        }

        let tree = Tree::builder(&self.base)
            .with_scheme(SCHEME)
            .with_no_extension()
            .with_prefix_part_lengths(lengths)
            .build()
            .map_err(|_| InitializationError::InvalidPrefixPartLengths(lengths.to_vec()))?;

        Ok(Self {
            base: self.base,
            tree,
        })
    }

    /// Infer the prefix part lengths that were used to create an existing store.
    ///
    /// Returns `None` if the store contains no files. The result is guaranteed
    /// to be correct when the store is valid, but validity is not verified.
    pub fn infer_prefix_part_lengths<P: AsRef<Path>>(
        base: P,
    ) -> Result<Option<Vec<usize>>, prefix_file_tree::Error> {
        Tree::<Identity>::infer_prefix_part_lengths(base)
    }

    /// Compute the storage path for `digest` without performing any I/O.
    #[must_use]
    pub fn path(&self, digest: Digest) -> PathBuf {
        // A 16-byte array is always a valid `Hex<16>` name, so this never fails.
        #[allow(clippy::missing_panics_doc)]
        self.tree
            .path(digest.0)
            .expect("Hex<16> path construction from [u8; 16] is infallible")
    }

    /// Return an iterator over every entry in the store.
    #[must_use]
    pub fn entries(&self) -> prefix_file_tree::iter::Entries<'_, Hex<16>> {
        self.tree.entries()
    }

    /// Verify the stored file's MD5 digest matches the recorded one.
    ///
    /// # Returns
    ///
    /// - `Ok(Ok(()))` — digest matches.
    /// - `Ok(Err(actual))` — digest mismatch; `actual` is what was computed.
    /// - `Err(e)` — I/O error reading the file.
    fn validate(entry: &Entry<[u8; 16]>) -> Result<Result<(), Digest>, std::io::Error> {
        let bytes = std::fs::read(&entry.path)?;
        let digest = md5::compute(&bytes);

        if digest.0 == entry.name {
            Ok(Ok(()))
        } else {
            Ok(Err(digest))
        }
    }

    /// Validate each entry by re-computing its MD5 digest.
    pub fn validate_entries(
        &self,
    ) -> impl Iterator<Item = Result<ValidationResult, prefix_file_tree::iter::Error>> {
        self.entries().map(|entry| {
            let entry = entry?;

            Ok(match Self::validate(&entry)? {
                Ok(()) => ValidationResult::Valid { entry },
                Err(actual) => ValidationResult::Invalid { entry, actual },
            })
        })
    }

    /// Like [`validate_entries`](Self::validate), but stops on the first invalid or corrupt entry.
    pub fn validate_entries_fail_fast(
        &self,
    ) -> impl Iterator<Item = Result<Entry<[u8; 16]>, ValidationError>> {
        self.validate_entries().map(|result| {
            result
                .map_err(ValidationError::from)
                .and_then(ValidationResult::result)
        })
    }

    /// Write `bytes` to the store and return an [`Action`] describing the outcome.
    ///
    /// If an identical digest already exists, the file is not re-written and `Action::added` is
    /// `false`.
    ///
    /// # Errors
    ///
    /// Returns an error if an I/O operation fails.
    pub fn save<T: AsRef<[u8]>>(&self, bytes: T) -> Result<Action, prefix_file_tree::Error> {
        let bytes = bytes.as_ref();

        // Image type detection requires at least 8 bytes for the magic number.
        let image_type = infer_image_format(bytes);

        let digest = md5::compute(bytes);
        let path = self.path(digest);

        let added = match self.tree.create_file(digest.0)? {
            Some(mut file) => {
                use std::io::Write as _;
                file.write_all(bytes).map_err(prefix_file_tree::Error::Io)?;
                true
            }
            None => false,
        };

        Ok(Action {
            entry: Entry {
                path,
                name: digest.0,
            },
            image_type,
            added,
        })
    }
}

/// Infer image type from bytes.
///
/// This is a hack that is necessary because the `imghdr` crate does not correctly handle some
/// apparently valid JPEG images served by Meta.
fn infer_image_format<T: AsRef<[u8]>>(bytes: T) -> ImageType {
    let bytes = bytes.as_ref();

    // Image type detection requires at least 8 bytes for the magic number.
    if bytes.len() < 8 {
        ImageType::new(None)
    } else {
        imghdr::from_bytes(bytes)
            .map(|image_type| ImageType::new(Some(image_type)))
            .or_else(|| {
                let mut cursor = std::io::Cursor::new(bytes);

                imageformat::detect_image_format(&mut cursor)
                    .ok()
                    .map(|image_format| match image_format {
                        imageformat::ImageFormat::Jpeg => ImageType::new(Some(imghdr::Type::Jpeg)),
                        _ => ImageType::new(None),
                    })
            })
            .unwrap_or_default()
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
    ) -> Result<Vec<prefix_file_tree::Entry<[u8; 16]>>, Box<dyn std::error::Error>> {
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
        let digests = entries.iter().map(|entry| entry.name).collect::<Vec<_>>();

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
