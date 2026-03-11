package archive

import (
	"archive/tar"
	"reflect"
	"strings"
	"testing"
	"time"
)

// TestEnsurePAXInitializesAndPreservesRecords verifies that EnsurePAX creates a
// record map when needed without discarding existing entries.
func TestEnsurePAXInitializesAndPreservesRecords(t *testing.T) {
	hdr := &tar.Header{}
	EnsurePAX(hdr)
	if hdr.PAXRecords == nil {
		t.Fatalf("EnsurePAX() left PAXRecords nil")
	}

	hdr = &tar.Header{PAXRecords: map[string]string{"existing": "value"}}
	EnsurePAX(hdr)
	if got := hdr.PAXRecords["existing"]; got != "value" {
		t.Fatalf("EnsurePAX() lost existing record, got %q", got)
	}
}

// TestEncodeDecodeXattrToPAXRoundTrip verifies that xattrs survive the PAX
// encoding and decoding round trip.
func TestEncodeDecodeXattrToPAXRoundTrip(t *testing.T) {
	attrs := map[string][]byte{
		"user.gotgz.simple":          []byte("value"),
		"user.gotgz.name with space": {0x00, 0x01, 0x02, 0x03},
	}
	hdr := &tar.Header{}

	EncodeXattrToPAX(hdr, attrs)
	got, err := DecodeXattrFromPAX(hdr)
	if err != nil {
		t.Fatalf("DecodeXattrFromPAX() error = %v", err)
	}
	if !reflect.DeepEqual(got, attrs) {
		t.Fatalf("DecodeXattrFromPAX() = %#v, want %#v", got, attrs)
	}
}

// TestDecodeXattrFromPAXRejectsInvalidInput verifies that malformed PAX xattr
// records fail with a decoding error instead of silently producing bad data.
func TestDecodeXattrFromPAXRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name string
		hdr  *tar.Header
	}{
		{
			name: "invalid escaped name",
			hdr: &tar.Header{PAXRecords: map[string]string{
				xattrPrefix + "%zz": "dmFsdWU=",
			}},
		},
		{
			name: "invalid base64 value",
			hdr: &tar.Header{PAXRecords: map[string]string{
				xattrPrefix + "user.gotgz.test": "%%%not-base64%%%",
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := DecodeXattrFromPAX(tt.hdr); err == nil {
				t.Fatalf("DecodeXattrFromPAX() error = nil, want non-nil")
			}
		})
	}
}

// TestEncodeDecodeACLToPAXRoundTrip verifies that ACL blobs survive the PAX
// encoding and decoding round trip.
func TestEncodeDecodeACLToPAXRoundTrip(t *testing.T) {
	acls := map[string][]byte{
		"user.gotgz.acl": []byte("acl-value"),
		"system.posix":   {0x10, 0x20, 0x30},
	}
	hdr := &tar.Header{}

	EncodeACLToPAX(hdr, acls)
	got, err := DecodeACLFromPAX(hdr)
	if err != nil {
		t.Fatalf("DecodeACLFromPAX() error = %v", err)
	}
	if !reflect.DeepEqual(got, acls) {
		t.Fatalf("DecodeACLFromPAX() = %#v, want %#v", got, acls)
	}
}

// TestDecodeACLFromPAXRejectsInvalidInput verifies that malformed ACL values
// are rejected during decoding.
func TestDecodeACLFromPAXRejectsInvalidInput(t *testing.T) {
	hdr := &tar.Header{PAXRecords: map[string]string{
		aclPrefix + "user.gotgz.acl": "%%%not-base64%%%",
	}}

	if _, err := DecodeACLFromPAX(hdr); err == nil {
		t.Fatalf("DecodeACLFromPAX() error = nil, want non-nil")
	}
}

// TestHeaderToS3Metadata verifies that tar header fields are mapped to S3
// metadata and that oversize metadata is flagged.
func TestHeaderToS3Metadata(t *testing.T) {
	t.Run("within limit includes optional fields", func(t *testing.T) {
		hdr := &tar.Header{
			Typeflag: tar.TypeSymlink,
			Mode:     0o750,
			Uid:      12,
			Gid:      34,
			ModTime:  time.Unix(1_700_000_000, 0).UTC(),
			Linkname: "target.txt",
			Uname:    "alice",
			Gname:    "staff",
		}

		got, ok := HeaderToS3Metadata(hdr)
		if !ok {
			t.Fatalf("HeaderToS3Metadata() reported oversize metadata unexpectedly")
		}
		want := map[string]string{
			"gotgz-type":     "50",
			"gotgz-mode":     "750",
			"gotgz-uid":      "12",
			"gotgz-gid":      "34",
			"gotgz-mtime":    "1700000000",
			"gotgz-linkname": "target.txt",
			"gotgz-uname":    "alice",
			"gotgz-gname":    "staff",
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("HeaderToS3Metadata() = %#v, want %#v", got, want)
		}
	})

	t.Run("oversize metadata is reported", func(t *testing.T) {
		hdr := &tar.Header{
			Typeflag: tar.TypeReg,
			Mode:     0o644,
			Linkname: strings.Repeat("x", 2_000),
		}

		_, ok := HeaderToS3Metadata(hdr)
		if ok {
			t.Fatalf("HeaderToS3Metadata() ok = true, want false for oversize metadata")
		}
	})
}

// TestParseMTime verifies that valid epoch values parse and invalid values
// fall back to the zero time.
func TestParseMTime(t *testing.T) {
	got := ParseMTime("1700000000")
	want := time.Unix(1_700_000_000, 0)
	if !got.Equal(want) {
		t.Fatalf("ParseMTime(valid) = %v, want %v", got, want)
	}

	if got := ParseMTime("not-a-time"); !got.IsZero() {
		t.Fatalf("ParseMTime(invalid) = %v, want zero time", got)
	}
}
