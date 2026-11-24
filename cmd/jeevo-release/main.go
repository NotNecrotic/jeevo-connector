package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/NotNecrotic/jeevo-connector/internal/protocol"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: jeevo-release keygen|manifest|sign|verify ...")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "keygen":
		err = keygen()
	case "manifest":
		err = manifest(os.Args[2:])
	case "sign":
		err = sign(os.Args[2:])
	case "verify":
		err = verify(os.Args[2:])
	default:
		err = errors.New("unknown command")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func keygen() error {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	fmt.Println("# Keep the private key offline. Put the public key in CONNECTOR_RELEASE_PUBLIC_KEYS on the Jeevo server.")
	fmt.Printf("private: %s\n", base64.StdEncoding.EncodeToString(priv.Seed()))
	fmt.Printf("public:  %s\n", base64.StdEncoding.EncodeToString(pub))
	return nil
}

var reArtifact = regexp.MustCompile(`^jeevo-connector-.+-(linux)-(amd64|arm64)\.tar\.gz$`)

func manifest(args []string) error {
	fs := flag.NewFlagSet("manifest", flag.ExitOnError)
	ver := fs.String("version", "", "release version")
	channel := fs.String("channel", "stable", "stable|beta")
	dir := fs.String("dir", "", "directory holding the tarballs")
	notes := fs.String("notes", "", "release notes")
	_ = fs.Parse(args)
	if *ver == "" || *dir == "" {
		return errors.New("--version and --dir are required")
	}
	ents, err := os.ReadDir(*dir)
	if err != nil {
		return err
	}
	m := protocol.ReleaseManifest{Version: *ver, Channel: protocol.ReleaseChannel(*channel), PublishedAt: time.Now().UTC().Truncate(time.Second), ProtocolVersion: protocol.ProtocolVersion}
	if *notes != "" {
		m.Notes = notes
	}
	for _, e := range ents {
		mm := reArtifact.FindStringSubmatch(e.Name())
		if mm == nil {
			continue
		}
		sum, n, err := hashFile(filepath.Join(*dir, e.Name()))
		if err != nil {
			return err
		}
		m.Artifacts = append(m.Artifacts, protocol.ReleaseArtifact{Os: protocol.ReleaseArtifactOs(mm[1]), Arch: protocol.ReleaseArtifactArch(mm[2]), File: e.Name(), Sha256: sum, Size: n})
	}
	if len(m.Artifacts) == 0 {
		return errors.New("no jeevo-connector-*-linux-<arch>.tar.gz files found")
	}
	if sum, n, err := hashFile(filepath.Join(*dir, "install.sh")); err == nil {
		m.InstallScript = &protocol.ReleaseInstallScript{File: protocol.InstallSh, Sha256: sum, Size: n}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	raw, _ := json.MarshalIndent(m, "", "  ")
	out := filepath.Join(*dir, "manifest.json")
	if err := os.WriteFile(out, raw, 0o644); err != nil {
		return err
	}
	fmt.Println("wrote", out)
	return nil
}

func hashFile(path string) (string, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), int(n), nil
}

func sign(args []string) error {
	fs := flag.NewFlagSet("sign", flag.ExitOnError)
	keyFile := fs.String("key", "", "file holding the base64 private key seed")
	path := fs.String("manifest", "", "manifest.json to sign")
	_ = fs.Parse(args)
	seedB64 := os.Getenv("JEEVO_RELEASE_KEY")
	if *keyFile != "" {
		raw, err := os.ReadFile(*keyFile)
		if err != nil {
			return err
		}
		seedB64 = strings.TrimSpace(string(raw))
	}
	seed, err := base64.StdEncoding.DecodeString(seedB64)
	if err != nil || len(seed) != ed25519.SeedSize {
		return errors.New("private key must be a base64 32-byte seed (from keygen), via --key or JEEVO_RELEASE_KEY")
	}
	data, err := os.ReadFile(*path)
	if err != nil {
		return err
	}
	sig := ed25519.Sign(ed25519.NewKeyFromSeed(seed), data)
	out := *path + ".sig"
	if err := os.WriteFile(out, []byte(base64.StdEncoding.EncodeToString(sig)+"\n"), 0o644); err != nil {
		return err
	}
	fmt.Println("wrote", out)
	return nil
}

func verify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	pubB64 := fs.String("pub", "", "base64 public key")
	path := fs.String("manifest", "", "manifest.json")
	sigPath := fs.String("sig", "", "manifest.json.sig")
	_ = fs.Parse(args)
	pub, err := base64.StdEncoding.DecodeString(*pubB64)
	if err != nil || len(pub) != ed25519.PublicKeySize {
		return errors.New("bad public key")
	}
	data, err := os.ReadFile(*path)
	if err != nil {
		return err
	}
	sraw, err := os.ReadFile(*sigPath)
	if err != nil {
		return err
	}
	sig, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(sraw)))
	if err != nil || !ed25519.Verify(ed25519.PublicKey(pub), data, sig) {
		return errors.New("signature does NOT verify")
	}
	fmt.Println("signature ok")
	return nil
}
