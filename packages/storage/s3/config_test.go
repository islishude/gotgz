package s3

import "testing"

// TestIntFromEnv verifies that integer environment settings are parsed only
// when present and valid.
func TestIntFromEnv(t *testing.T) {
	t.Setenv("GOTGZ_TEST_INT", " 42 ")
	if got, ok := intFromEnv("GOTGZ_TEST_INT"); !ok || got != 42 {
		t.Fatalf("intFromEnv(valid) = (%d, %t), want (42, true)", got, ok)
	}

	t.Setenv("GOTGZ_TEST_INT", "bad")
	if got, ok := intFromEnv("GOTGZ_TEST_INT"); ok || got != 0 {
		t.Fatalf("intFromEnv(invalid) = (%d, %t), want (0, false)", got, ok)
	}

	t.Setenv("GOTGZ_TEST_INT", "")
	if got, ok := intFromEnv("GOTGZ_TEST_INT"); ok || got != 0 {
		t.Fatalf("intFromEnv(empty) = (%d, %t), want (0, false)", got, ok)
	}
}

// TestInt64FromEnv verifies that int64 environment settings are parsed only
// when present and valid.
func TestInt64FromEnv(t *testing.T) {
	t.Setenv("GOTGZ_TEST_INT64", " 4096 ")
	if got, ok := int64FromEnv("GOTGZ_TEST_INT64"); !ok || got != 4096 {
		t.Fatalf("int64FromEnv(valid) = (%d, %t), want (4096, true)", got, ok)
	}

	t.Setenv("GOTGZ_TEST_INT64", "bad")
	if got, ok := int64FromEnv("GOTGZ_TEST_INT64"); ok || got != 0 {
		t.Fatalf("int64FromEnv(invalid) = (%d, %t), want (0, false)", got, ok)
	}

	t.Setenv("GOTGZ_TEST_INT64", "")
	if got, ok := int64FromEnv("GOTGZ_TEST_INT64"); ok || got != 0 {
		t.Fatalf("int64FromEnv(empty) = (%d, %t), want (0, false)", got, ok)
	}
}
