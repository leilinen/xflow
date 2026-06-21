/// Returns true if the text is primarily Chinese (CJK Unified Ideographs
/// make up more than 30% of non-whitespace characters).
///
/// Mixed Chinese/English tweets with sufficient Chinese content are treated
/// as Chinese (no need to re-translate). Pure English, Japanese kana-only,
/// and other non-Chinese text return false.
pub fn is_primarily_chinese(text: &str) -> bool {
    let mut total_chars = 0usize;
    let mut cjk_chars = 0usize;

    for ch in text.chars() {
        if ch.is_whitespace() {
            continue;
        }
        total_chars += 1;
        if is_cjk_ideograph(ch) {
            cjk_chars += 1;
        }
    }

    if total_chars == 0 {
        return false;
    }

    cjk_chars as f64 / total_chars as f64 > 0.3
}

fn is_cjk_ideograph(ch: char) -> bool {
    matches!(ch,
        '\u{4E00}'..='\u{9FFF}' |  // CJK Unified Ideographs
        '\u{3400}'..='\u{4DBF}' |  // CJK Unified Ideographs Extension A
        '\u{F900}'..='\u{FAFF}'    // CJK Compatibility Ideographs
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pure_chinese() {
        assert!(is_primarily_chinese("今天天气真好，适合出去散步"));
    }

    #[test]
    fn pure_english() {
        assert!(!is_primarily_chinese("The weather is nice today"));
    }

    #[test]
    fn mixed_chinese_english() {
        // >30% CJK — should be considered Chinese
        assert!(is_primarily_chinese("今天用了Claude来写代码，效果很好"));
    }

    #[test]
    fn mostly_english_with_chinese_phrase() {
        // Only 2 CJK chars out of many — not primarily Chinese
        assert!(!is_primarily_chinese(
            "Just shipped the 你好 feature to production"
        ));
    }

    #[test]
    fn empty_string() {
        assert!(!is_primarily_chinese(""));
    }

    #[test]
    fn emojis_only() {
        assert!(!is_primarily_chinese("🚀🎉💯"));
    }

    #[test]
    fn japanese_hiragana_not_chinese() {
        // Hiragana are not CJK ideographs
        assert!(!is_primarily_chinese("こんにちは世界"));
    }

    #[test]
    fn chinese_with_emojis() {
        assert!(is_primarily_chinese("今天天气真好 🎉🎉"));
    }
}
