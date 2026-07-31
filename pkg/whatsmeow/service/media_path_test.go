package whatsmeow_service

import (
	"strings"
	"testing"
)

func TestBuildMediaFileNamePrefixaInstancia(t *testing.T) {
	got := buildMediaFileName("0c9a1f2e-5f4b-4a3c-9d0e-8b7a6c5d4e3f", "3EB0ABCDEF.jpg")

	want := "0c9a1f2e-5f4b-4a3c-9d0e-8b7a6c5d4e3f/3EB0ABCDEF.jpg"
	if got != want {
		t.Fatalf("esperava %q, veio %q", want, got)
	}
}

func TestBuildMediaFileNameCaiNoFormatoAntigoSemInstancia(t *testing.T) {
	casos := []string{"", "   ", "///", "..", "   ..  "}

	for _, instanceID := range casos {
		if got := buildMediaFileName(instanceID, "3EB0ABCDEF.jpg"); got != "3EB0ABCDEF.jpg" {
			t.Fatalf("instanceID %q: esperava o formato antigo %q, veio %q", instanceID, "3EB0ABCDEF.jpg", got)
		}
	}
}

func TestBuildMediaFileNameNuncaGeraMaisDeUmNivel(t *testing.T) {
	casos := []string{
		"../../etc",
		"tenant/sub",
		"tenant\\sub",
		"com espaço",
		"instância-á",
		"a\tb",
		"..",
	}

	for _, instanceID := range casos {
		got := buildMediaFileName(instanceID, "ABC.jpg")
		if strings.Count(got, "/") > 1 {
			t.Fatalf("instanceID %q gerou mais de um nível: %q", instanceID, got)
		}
		if strings.Contains(got, "..") {
			t.Fatalf("instanceID %q deixou %q no caminho: %q", instanceID, "..", got)
		}
		if !strings.HasSuffix(got, "ABC.jpg") {
			t.Fatalf("instanceID %q perdeu o nome do arquivo: %q", instanceID, got)
		}
	}
}

func TestSanitizeInstanceSegmentTrocaCaractereInvalido(t *testing.T) {
	casos := map[string]string{
		"0c9a1f2e-5f4b": "0c9a1f2e-5f4b",
		"tenant_01":     "tenant_01",
		"tenant/sub":    "tenant_sub",
		"../etc":        "___etc",
		"com espaço":    "com_espa_o",
		"..":            "",
		"":              "",
		"   ":           "",
		"__":            "",
	}

	for entrada, want := range casos {
		if got := sanitizeInstanceSegment(entrada); got != want {
			t.Fatalf("entrada %q: esperava %q, veio %q", entrada, want, got)
		}
	}
}

func TestSanitizeInstanceSegmentRespeitaTeto(t *testing.T) {
	got := sanitizeInstanceSegment(strings.Repeat("a", maxInstanceSegmentLength+50))

	if len(got) != maxInstanceSegmentLength {
		t.Fatalf("esperava %d caracteres, veio %d", maxInstanceSegmentLength, len(got))
	}
}
