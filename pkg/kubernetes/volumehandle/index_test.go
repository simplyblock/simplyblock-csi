package volumehandle

import "testing"

const (
	testCluster  = "8ffac363-0c46-4714-a71b-f9c0b58a1269"
	testPoolUUID = "df34f16c-1a2b-3c4d-5e6f-7a8b9c0d1e2f"
	testLvol     = "8e2dcb9d-1b2c-4f3a-9d4e-5f6a7b8c9d0e"
)

func parseCases() []struct {
	name    string
	handle  string
	wantOK  bool
	cluster string
	pool    string
	lvol    string
} {
	return []struct {
		name    string
		handle  string
		wantOK  bool
		cluster string
		pool    string
		lvol    string
	}{
		{
			name:   "all UUIDs",
			handle: testCluster + ":" + testPoolUUID + ":" + testLvol,
			wantOK: true, cluster: testCluster, pool: testPoolUUID, lvol: testLvol,
		},
		{
			name:   "pool is a name",
			handle: testCluster + ":my-pool:" + testLvol,
			wantOK: true, cluster: testCluster, pool: "my-pool", lvol: testLvol,
		},
		{
			name:   "surrounding whitespace trimmed",
			handle: "  " + testCluster + ":my-pool:" + testLvol + "  ",
			wantOK: true, cluster: testCluster, pool: "my-pool", lvol: testLvol,
		},
		{name: "empty handle", handle: ""},
		{name: "two parts", handle: testCluster + ":" + testLvol},
		{name: "four parts", handle: testCluster + ":p:" + testLvol + ":extra"},
		{name: "cluster not a UUID", handle: "not-a-uuid:my-pool:" + testLvol},
		{name: "lvol not a UUID", handle: testCluster + ":my-pool:not-a-uuid"},
		{name: "empty pool", handle: testCluster + "::" + testLvol},
		{name: "empty cluster", handle: ":my-pool:" + testLvol},
		{name: "empty lvol", handle: testCluster + ":my-pool:"},
	}
}

func TestParse(t *testing.T) {
	for _, tc := range parseCases() {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := Parse(tc.handle)
			if ok != tc.wantOK {
				t.Fatalf("Parse(%q) ok = %v, want %v", tc.handle, ok, tc.wantOK)
			}
			if !tc.wantOK {
				if got != NilVolumeHandle {
					t.Fatalf("Parse(%q) returned %+v on failure, want zero value", tc.handle, got)
				}
				return
			}
			if got.ClusterID != tc.cluster || got.PoolID != tc.pool || got.VolumeID != tc.lvol {
				t.Fatalf("Parse(%q) = %+v, want {%q %q %q}",
					tc.handle, got, tc.cluster, tc.pool, tc.lvol)
			}
		})
	}
}

func TestMustParse(t *testing.T) {
	for _, tc := range parseCases() {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if tc.wantOK && r != nil {
					t.Fatalf("MustParse(%q) panicked unexpectedly: %v", tc.handle, r)
				}
				if !tc.wantOK && r == nil {
					t.Fatalf("MustParse(%q) did not panic, want panic", tc.handle)
				}
			}()
			got := MustParse(tc.handle)
			if got.ClusterID != tc.cluster || got.PoolID != tc.pool || got.VolumeID != tc.lvol {
				t.Fatalf("MustParse(%q) = %+v, want {%q %q %q}",
					tc.handle, got, tc.cluster, tc.pool, tc.lvol)
			}
		})
	}
}

func TestIsUUID(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want bool
	}{
		{"lowercase", "8ffac363-0c46-4714-a71b-f9c0b58a1269", true},
		{"uppercase", "8FFAC363-0C46-4714-A71B-F9C0B58A1269", true},
		{"mixed case", "8ffAC363-0c46-4714-A71b-f9C0b58a1269", true},
		{"empty", "", false},
		{"pool name", "my-pool", false},
		{"missing hyphens", "8ffac3630c464714a71bf9c0b58a1269", false},
		{"too short", "8ffac363-0c46-4714-a71b-f9c0b58a126", false},
		{"too long", "8ffac363-0c46-4714-a71b-f9c0b58a12690", false},
		{"non-hex digit", "8ffac363-0c46-4714-a71b-f9c0b58a126g", false},
		{"hyphens misplaced", "8ffac3630-c46-4714-a71b-f9c0b58a1269", false},
		{"leading space", " 8ffac363-0c46-4714-a71b-f9c0b58a1269", false},
		{"trailing space", "8ffac363-0c46-4714-a71b-f9c0b58a1269 ", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isUUID(tc.in); got != tc.want {
				t.Fatalf("isUUID(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}
