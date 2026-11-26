package send_service

import (
	"bytes"
	"image"
	_ "image/jpeg"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestMetadadosDeVideo_CaminhoInexistente(t *testing.T) {
	segundos, largura, altura, miniatura := metadadosDeVideo(filepath.Join(t.TempDir(), "nao-existe.mp4"))
	if segundos != 0 || largura != 0 || altura != 0 || miniatura != nil {
		t.Fatalf("esperado zeros/nil para caminho inexistente, veio segundos=%d largura=%d altura=%d miniatura=%d bytes",
			segundos, largura, altura, len(miniatura))
	}

	segundos, largura, altura, miniatura = metadadosDeVideo("")
	if segundos != 0 || largura != 0 || altura != 0 || miniatura != nil {
		t.Fatalf("esperado zeros/nil para caminho vazio, veio segundos=%d largura=%d altura=%d miniatura=%d bytes",
			segundos, largura, altura, len(miniatura))
	}
}

func TestMetadadosDeVideoDeLeitor_NilOuLixoNaoQuebraEReposiciona(t *testing.T) {
	segundos, largura, altura, miniatura := metadadosDeVideoDeLeitor(nil)
	if segundos != 0 || largura != 0 || altura != 0 || miniatura != nil {
		t.Fatalf("esperado zeros/nil para leitor nil")
	}

	r := bytes.NewReader([]byte("conteudo invalido que nao eh mp4"))
	if _, err := r.Seek(5, io.SeekStart); err != nil {
		t.Fatalf("seek inicial falhou: %v", err)
	}
	segundos, largura, altura, miniatura = metadadosDeVideoDeLeitor(r)
	if segundos != 0 || largura != 0 || altura != 0 || miniatura != nil {
		t.Fatalf("esperado zeros/nil para entrada invalida")
	}
	pos, err := r.Seek(0, io.SeekCurrent)
	if err != nil || pos != 0 {
		t.Fatalf("esperado leitor reposicionado no inicio (0), veio pos=%d err=%v", pos, err)
	}
}

func TestMetadadosDeVideo_MP4ComFFmpeg(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg não instalado; pulando teste de metadados de vídeo")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe não instalado; pulando teste de metadados de vídeo")
	}

	caminho := filepath.Join(t.TempDir(), "amostra.mp4")
	cmd := exec.Command("ffmpeg",
		"-y",
		"-f", "lavfi",
		"-i", "testsrc=size=320x240:rate=10",
		"-t", "2",
		"-pix_fmt", "yuv420p",
		caminho,
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("falha ao gerar mp4 de teste: %v\n%s", err, string(out))
	}

	segundos, largura, altura, miniatura := metadadosDeVideo(caminho)
	if largura != 320 || altura != 240 || segundos != 2 {
		t.Fatalf("metadados inesperados: largura=%d altura=%d segundos=%d (esperado 320x240, 2s)",
			largura, altura, segundos)
	}
	if len(miniatura) == 0 {
		t.Fatal("esperada miniatura JPEG não vazia")
	}
	cfg, formato, err := image.DecodeConfig(bytes.NewReader(miniatura))
	if err != nil {
		t.Fatalf("miniatura não é imagem decodificável: %v", err)
	}
	if formato != "jpeg" || cfg.Width < 1 || cfg.Height < 1 {
		t.Fatalf("miniatura inesperada: formato=%q %dx%d", formato, cfg.Width, cfg.Height)
	}

	// Confere leitura via *os.File e reposicionamento no início.
	arq, err := os.Open(caminho)
	if err != nil {
		t.Fatalf("falha ao abrir amostra: %v", err)
	}
	defer arq.Close()
	if _, err := arq.Seek(10, io.SeekStart); err != nil {
		t.Fatalf("seek em arquivo falhou: %v", err)
	}
	s2, l2, a2, m2 := metadadosDeVideoDeLeitor(arq)
	if l2 != 320 || a2 != 240 || s2 != 2 || len(m2) == 0 {
		t.Fatalf("metadados via *os.File inesperados: %dx%d %ds thumb=%d", l2, a2, s2, len(m2))
	}
	if pos, err := arq.Seek(0, io.SeekCurrent); err != nil || pos != 0 {
		t.Fatalf("esperado *os.File reposicionado no inicio, veio pos=%d err=%v", pos, err)
	}

	// Confere leitura via io.ReadSeeker genérico (cópia em temporário) e reposicionamento.
	dados, err := os.ReadFile(caminho)
	if err != nil {
		t.Fatalf("falha ao ler amostra em bytes: %v", err)
	}
	br := bytes.NewReader(dados)
	s3, l3, a3, m3 := metadadosDeVideoDeLeitor(br)
	if l3 != 320 || a3 != 240 || s3 != 2 || len(m3) == 0 {
		t.Fatalf("metadados via bytes.Reader inesperados: %dx%d %ds thumb=%d", l3, a3, s3, len(m3))
	}
	if pos, err := br.Seek(0, io.SeekCurrent); err != nil || pos != 0 {
		t.Fatalf("esperado bytes.Reader reposicionado no inicio, veio pos=%d err=%v", pos, err)
	}
}

func TestMensagemDeVideoComMetadados(t *testing.T) {
	up := uploadDeTeste()
	up.StreamingSidecar = []byte{9, 8, 7}

	media, tipo := mensagemDeVideoComMetadados(&MediaStruct{Caption: "legenda"}, up, "video/mp4", 1234, false, 15, 1920, 1080, []byte{0xff, 0xd8})
	if tipo != "VideoMessage" {
		t.Fatalf("tipo = %q", tipo)
	}
	v := media.GetVideoMessage()
	if v.GetMediaKeyTimestamp() == 0 {
		t.Error("esperado MediaKeyTimestamp preenchido")
	}
	if !bytes.Equal(v.GetStreamingSidecar(), []byte{9, 8, 7}) {
		t.Errorf("StreamingSidecar = %v", v.GetStreamingSidecar())
	}
	if v.GetSeconds() != 15 || v.GetWidth() != 1920 || v.GetHeight() != 1080 {
		t.Errorf("dimensões/duração erradas: %dx%d %ds", v.GetWidth(), v.GetHeight(), v.GetSeconds())
	}
	if !bytes.Equal(v.GetJPEGThumbnail(), []byte{0xff, 0xd8}) {
		t.Errorf("JPEGThumbnail = %v", v.GetJPEGThumbnail())
	}

	// Campos zero/nil devem ficar ausentes.
	upSemSidecar := uploadDeTeste()
	mediaZero, _ := mensagemDeVideoComMetadados(&MediaStruct{}, upSemSidecar, "video/mp4", 100, false, 0, 0, 0, nil)
	vz := mediaZero.GetVideoMessage()
	if vz.Seconds != nil || vz.Width != nil || vz.Height != nil || vz.JPEGThumbnail != nil || vz.StreamingSidecar != nil {
		t.Errorf("campos zero/nil deveriam estar ausentes: %+v", vz)
	}
	if vz.MediaKeyTimestamp == nil {
		t.Error("MediaKeyTimestamp deve estar presente mesmo sem metadados")
	}

	// Newsletter não leva MediaKeyTimestamp, StreamingSidecar nem metadados extras.
	mediaNews, _ := mensagemDeVideoComMetadados(&MediaStruct{}, up, "video/mp4", 100, true, 15, 1920, 1080, []byte{1})
	vn := mediaNews.GetVideoMessage()
	if vn.MediaKeyTimestamp != nil || vn.StreamingSidecar != nil || vn.Seconds != nil || vn.Width != nil || vn.Height != nil || vn.JPEGThumbnail != nil {
		t.Errorf("newsletter não deve levar chave/sidecar/metadados: %+v", vn)
	}
}

func TestMensagemDeDocumento_MediaKeyTimestamp(t *testing.T) {
	media, _ := mensagemDeDocumento(&MediaStruct{Filename: "a.mp4"}, uploadDeTeste(), "video/mp4", 50, false, []byte{1, 2})
	doc := media.GetDocumentMessage()
	if doc.GetMediaKeyTimestamp() == 0 {
		t.Error("esperado MediaKeyTimestamp em documento não newsletter")
	}
	if !bytes.Equal(doc.GetJPEGThumbnail(), []byte{1, 2}) {
		t.Errorf("JPEGThumbnail = %v", doc.GetJPEGThumbnail())
	}

	mediaNews, _ := mensagemDeDocumento(&MediaStruct{Filename: "a.mp4"}, uploadDeTeste(), "video/mp4", 50, true, nil)
	if mediaNews.GetDocumentMessage().MediaKeyTimestamp != nil {
		t.Error("newsletter não deve levar MediaKeyTimestamp")
	}
}
