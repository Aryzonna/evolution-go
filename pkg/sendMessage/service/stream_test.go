package send_service

import (
	"bytes"
	"testing"

	"go.mau.fi/whatsmeow"
)

func uploadDeTeste() whatsmeow.UploadResponse {
	return whatsmeow.UploadResponse{
		URL:           "https://mmg.whatsapp.net/x",
		DirectPath:    "/v/x",
		MediaKey:      []byte{1, 2, 3},
		FileEncSHA256: []byte{4},
		FileSHA256:    []byte{5},
		FileLength:    10,
	}
}

func TestSobeEmStream(t *testing.T) {
	for tipo, esperado := range map[string]bool{
		"video":    true,
		"document": true,
		"image":    false,
		"audio":    false,
		"ptv":      false,
	} {
		if got := sobeEmStream(tipo); got != esperado {
			t.Errorf("sobeEmStream(%q) = %v, esperado %v", tipo, got, esperado)
		}
	}
}

func TestMensagemDeDocumentoComLegendaViraDocumentWithCaption(t *testing.T) {
	data := &MediaStruct{Filename: "contrato.pdf", Caption: "segue"}
	media, tipo := mensagemDeDocumento(data, uploadDeTeste(), "application/pdf", 2_000_000_000, false, nil)

	if tipo != "DocumentMessage" {
		t.Fatalf("tipo = %q", tipo)
	}
	if media.DocumentMessage != nil {
		t.Fatal("com legenda o DocumentMessage deveria ir dentro de DocumentWithCaptionMessage")
	}
	doc := media.GetDocumentWithCaptionMessage().GetMessage().GetDocumentMessage()
	if doc.GetFileLength() != 2_000_000_000 {
		t.Errorf("FileLength = %d, esperado o tamanho passado", doc.GetFileLength())
	}
	if doc.GetFileName() != "contrato.pdf" || len(doc.GetMediaKey()) == 0 {
		t.Errorf("campos do documento errados: %+v", doc)
	}
}

func TestMensagemDeDocumentoSemLegenda(t *testing.T) {
	media, _ := mensagemDeDocumento(&MediaStruct{Filename: "a.zip"}, uploadDeTeste(), "application/zip", 42, false, nil)
	if media.GetDocumentMessage().GetFileLength() != 42 {
		t.Fatalf("sem legenda o documento fica em DocumentMessage com o tamanho passado: %+v", media)
	}
}

func TestMensagemDeDocumentoNewsletterSemChave(t *testing.T) {
	media, _ := mensagemDeDocumento(&MediaStruct{Filename: "a.zip"}, uploadDeTeste(), "application/zip", 42, true, nil)
	if len(media.GetDocumentMessage().GetMediaKey()) != 0 {
		t.Fatal("newsletter não leva MediaKey")
	}
}

func TestMensagemDeVideo(t *testing.T) {
	media, tipo := mensagemDeVideo(&MediaStruct{Caption: "oi"}, uploadDeTeste(), "video/mp4", 500_000_000, false)
	if tipo != "VideoMessage" {
		t.Fatalf("tipo = %q", tipo)
	}
	v := media.GetVideoMessage()
	if v.GetFileLength() != 500_000_000 || v.GetCaption() != "oi" || len(v.GetMediaKey()) == 0 {
		t.Errorf("vídeo montado errado: %+v", v)
	}
}

func TestMakePDFThumbnailFromReaderComLixoNaoQuebra(t *testing.T) {
	if thumb := makePDFThumbnailFromReader(bytes.NewReader([]byte("isto não é um PDF")), 200); thumb != nil {
		t.Fatalf("esperado nil para entrada que não é PDF, veio %d bytes", len(thumb))
	}
}
