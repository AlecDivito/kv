#[derive(Debug)]
enum Test {
    Exact(u8),
    Wildcard,
    Until(Option<u8>),
}

#[derive(Debug)]
pub struct PreparedPattern {
    tests: Vec<Test>,
}

impl PreparedPattern {
    pub fn test(&self, input: &[u8]) -> bool {
        let mut iter = input.iter();
        for test in self.tests.iter() {
            let result = match test {
                Test::Exact(byte) => iter.next().map(|by| by == byte).unwrap_or(false),
                Test::Wildcard => iter.next().is_some(),
                Test::Until(None) => {
                    for _ in iter.by_ref() {}
                    true
                }
                Test::Until(Some(until)) => {
                    for byte in iter.by_ref() {
                        if *byte == *until {
                            break;
                        }
                    }
                    true
                }
            };
            if !result {
                return false;
            }
        }
        iter.next().is_none()
    }
}

pub fn prepare(like: Vec<u8>) -> PreparedPattern {
    let mut tests = vec![];
    let mut iter = like.into_iter();
    while let Some(byte) = iter.next() {
        match byte {
            b'*' => tests.push(Test::Until(iter.next())),
            b'_' => tests.push(Test::Wildcard),
            by => tests.push(Test::Exact(by)),
        }
    }
    PreparedPattern { tests }
}

#[cfg(test)]
mod tests {
    use super::prepare;

    #[test]
    fn match_all_exact() {
        let prepare = prepare(b"exact".to_vec());
        assert!(prepare.test(b"exact"));
        assert!(!prepare.test(b"exactt"));
        assert!(!prepare.test(b"eexact"));
        assert!(!prepare.test(b"exaat"));
        assert!(!prepare.test(b"bananas"));
    }

    #[test]
    fn match_all_wildcard() {
        let prepare = prepare(b"___".to_vec());
        assert!(prepare.test(b"any"));
        assert!(prepare.test(b"bun"));
        assert!(prepare.test(b"!@#"));
        assert!(prepare.test(b"123"));
        assert!(!prepare.test(b"bananas"));
        assert!(!prepare.test(b"b"));
    }

    #[test]
    fn match_all_any() {
        let prepare = prepare(b"ca*".to_vec());
        assert!(prepare.test(b"cats"));
        assert!(prepare.test(b"catermerage"));
        assert!(prepare.test(b"cater"));
        assert!(!prepare.test(b"alec"));
        assert!(!prepare.test(b""));
    }

    #[test]
    fn match_all_wildcard_and_any() {
        let prepare = prepare(b"___*".to_vec());
        assert!(prepare.test(b"any"));
        assert!(prepare.test(b"bananas"));
        assert!(!prepare.test(b""));
    }

    #[test]
    fn match_complex_pattern() {
        let prepare = prepare(b"th__ * is *ing".to_vec());
        assert!(prepare.test(b"that really is amazing"));
        assert!(prepare.test(b"them peppers is something"));
        assert!(prepare.test(b"thou  is crazying"))
    }

    #[test]
    fn match_complex_any_pattern() {
        let prepare = prepare(b"*82__".to_vec());
        assert!(prepare.test(b"Key8200"));
    }
}
