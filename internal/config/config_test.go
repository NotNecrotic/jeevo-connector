package config

import (
	"path/filepath"
	"testing"
)

func TestDefaultsValidate(t *testing.T) {
	s := DefaultSettings()
	if err := ValidateSettings(&s); err != nil {
		t.Fatalf("defaults invalid: %v", err)
	}
}

func TestDefaultPorts(t *testing.T) {
	s := DefaultSettings()
	if s.Network.ListenPort != 28443 || s.Network.PublicPort != 28443 || s.Network.PublicAddress != "" {
		t.Fatalf("unexpected network defaults: %+v", s.Network)
	}
}

func TestValidateRejects(t *testing.T) {
	mut := []func(*Settings){
		func(s *Settings) { s.Network.Mode = "sideways" },
		func(s *Settings) { s.Network.ListenPort = 0 },
		func(s *Settings) { s.Network.PublicAddress = "not-an-ip" },
		func(s *Settings) { s.Processing.HlsQualities = []int{721} },
		func(s *Settings) { s.Processing.HlsQualities = nil },
		func(s *Settings) { s.Network.PublicPort = 70000 },
		func(s *Settings) { s.Uploads.MaxUploadMb = 0 },
		func(s *Settings) { s.Processing.HlsEncoder = "h264_vaapi" },
		func(s *Settings) { s.Ai.Device = "tpu" },
		func(s *Settings) { s.Logging.Level = "loud" },
	}
	for i, m := range mut {
		s := DefaultSettings()
		m(&s)
		if ValidateSettings(&s) == nil {
			t.Errorf("mutation %d accepted", i)
		}
	}
}

func TestNormalizeAndPersist(t *testing.T) {
	s := DefaultSettings()
	s.Processing.HlsQualities = []int{360, 1080, 720, 360}
	NormalizeSettings(&s)
	if q := s.Processing.HlsQualities; len(q) != 3 || q[0] != 1080 || q[2] != 360 {
		t.Fatalf("normalize: %v", q)
	}
	p := filepath.Join(t.TempDir(), "settings.json")
	if err := SaveSettings(p, StoredSettings{Revision: 4, Settings: s}); err != nil {
		t.Fatal(err)
	}
	if got := LoadSettings(p); got.Revision != 4 {
		t.Fatalf("revision %d", got.Revision)
	}
	if got := LoadSettings(filepath.Join(t.TempDir(), "missing.json")); got.Revision != 0 {
		t.Fatal("missing file should give defaults")
	}
}
