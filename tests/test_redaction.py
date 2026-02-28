import unittest

from controller.redaction import mask_sensitive_text


class RedactionTestCase(unittest.TestCase):
    def test_mask_sensitive_text_masks_key_value_pairs(self) -> None:
        raw = 'auth_token=abcdef password:"p@ss" api_key: k-123'
        masked = mask_sensitive_text(raw)
        self.assertIn("auth_token=***", masked)
        self.assertIn('password:"***"', masked)
        self.assertIn("api_key: ***", masked)

    def test_mask_sensitive_text_masks_bearer_tokens(self) -> None:
        raw = (
            "Authorization: Bearer abcdef.123456\n"
            '{"authorization":"Bearer very-secret-token-value"}'
        )
        masked = mask_sensitive_text(raw)
        self.assertIn("Authorization: Bearer ***", masked)
        self.assertIn('"authorization":"Bearer ***"', masked)
        self.assertNotIn("very-secret-token-value", masked)


if __name__ == "__main__":
    unittest.main()
