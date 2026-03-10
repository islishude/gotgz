package archivepath

import "testing"

// TestParseSplit reports parsed fields for supported split archive names.
func TestParseSplit(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		want      SplitInfo
		wantFound bool
	}{
		{
			name:  "local tar gzip",
			input: "/tmp/backup.part0007.tar.gz",
			want: SplitInfo{
				DirPrefix: "/tmp/",
				Stem:      "backup",
				Suffix:    ".tar.gz",
				Part:      7,
				Width:     4,
			},
			wantFound: true,
		},
		{
			name:  "no suffix",
			input: "backup.part12",
			want: SplitInfo{
				DirPrefix: "",
				Stem:      "backup",
				Suffix:    "",
				Part:      12,
				Width:     2,
			},
			wantFound: true,
		},
		{
			name:  "windows path",
			input: `C:\tmp\backup.part0010.tar.zst`,
			want: SplitInfo{
				DirPrefix: `C:\tmp\`,
				Stem:      "backup",
				Suffix:    ".tar.zst",
				Part:      10,
				Width:     4,
			},
			wantFound: true,
		},
		{
			name:      "not split",
			input:     "backup.tar.gz",
			wantFound: false,
		},
		{
			name:      "part in middle only",
			input:     "backup-part0001.tar.gz",
			wantFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ParseSplit(tt.input)
			if ok != tt.wantFound {
				t.Fatalf("ParseSplit(%q) found = %v, want %v", tt.input, ok, tt.wantFound)
			}
			if !tt.wantFound {
				return
			}
			if got != tt.want {
				t.Fatalf("ParseSplit(%q) = %#v, want %#v", tt.input, got, tt.want)
			}
		})
	}
}

// TestFormatSplit inserts the part number before the full archive suffix.
func TestFormatSplit(t *testing.T) {
	tests := []struct {
		name  string
		input string
		part  int
		width int
		want  string
	}{
		{
			name:  "tar gzip keeps full suffix",
			input: "archive.tar.gz",
			part:  1,
			width: 2,
			want:  "archive.part0001.tar.gz",
		},
		{
			name:  "no suffix appends part",
			input: "archive",
			part:  12,
			width: 4,
			want:  "archive.part0012",
		},
		{
			name:  "directory preserved",
			input: "/var/backups/archive.tar.zst",
			part:  12345,
			width: 4,
			want:  "/var/backups/archive.part12345.tar.zst",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FormatSplit(tt.input, tt.part, tt.width); got != tt.want {
				t.Fatalf("FormatSplit(%q, %d, %d) = %q, want %q", tt.input, tt.part, tt.width, got, tt.want)
			}
		})
	}
}

// TestMatchSplit accepts only paths from the same split archive group.
func TestMatchSplit(t *testing.T) {
	want, ok := ParseSplit("/tmp/archive.part0001.tar.gz")
	if !ok {
		t.Fatal("ParseSplit() expected true for wanted archive")
	}

	tests := []struct {
		name      string
		candidate string
		wantFound bool
		wantPart  int
	}{
		{
			name:      "same group next part",
			candidate: "/tmp/archive.part0002.tar.gz",
			wantFound: true,
			wantPart:  2,
		},
		{
			name:      "different directory",
			candidate: "/opt/archive.part0002.tar.gz",
			wantFound: false,
		},
		{
			name:      "different suffix",
			candidate: "/tmp/archive.part0002.tar.zst",
			wantFound: false,
		},
		{
			name:      "different stem",
			candidate: "/tmp/other.part0002.tar.gz",
			wantFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, found := MatchSplit(tt.candidate, want)
			if found != tt.wantFound {
				t.Fatalf("MatchSplit(%q) found = %v, want %v", tt.candidate, found, tt.wantFound)
			}
			if !tt.wantFound {
				return
			}
			if got.Part != tt.wantPart {
				t.Fatalf("MatchSplit(%q) part = %d, want %d", tt.candidate, got.Part, tt.wantPart)
			}
		})
	}
}
