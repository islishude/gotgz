package locator

import "testing"

func TestParseArchiveHTTPURI(t *testing.T) {
	ref, err := ParseArchive("http://example.com/a.tar")
	if err != nil {
		t.Fatalf("ParseArchive() error = %v", err)
	}
	if ref.Kind != KindHTTP || ref.URL != "http://example.com/a.tar" {
		t.Fatalf("unexpected ref: %+v", ref)
	}
}

func TestParseArchiveHTTPSURI(t *testing.T) {
	ref, err := ParseArchive("https://example.com/a.tar.gz?sig=token")
	if err != nil {
		t.Fatalf("ParseArchive() error = %v", err)
	}
	if ref.Kind != KindHTTP || ref.URL != "https://example.com/a.tar.gz?sig=token" {
		t.Fatalf("unexpected ref: %+v", ref)
	}
}

func TestParseArchiveInvalidHTTPURI(t *testing.T) {
	_, err := ParseArchive("http:///a.tar")
	if err == nil {
		t.Fatalf("expected error")
	}
}
