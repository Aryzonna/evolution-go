package send_service

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"os"
	"os/exec"
	"strconv"
	"time"
)

const timeoutMetadadosVideo = 30 * time.Second

/*
metadadosDeVideo extrai duração, dimensões e miniatura JPEG de um vídeo em disco.

Celulares recusam vídeos enviados só com URL/chaves sem duração, largura/altura ou
miniatura. Qualquer falha (ffprobe/ffmpeg ausente, arquivo corrompido, timeout)
devolve zeros/nil para nunca impedir o envio do vídeo.
*/
func metadadosDeVideo(caminho string) (segundos, largura, altura uint32, miniatura []byte) {
	if caminho == "" {
		return 0, 0, 0, nil
	}

	segundos, largura, altura = extrairDimensoesEDuracao(caminho)
	miniatura = extrairMiniaturaDeVideo(caminho)
	return segundos, largura, altura, miniatura
}

/*
metadadosDeVideoDeLeitor extrai os metadados de um vídeo a partir de um io.ReadSeeker.

Quando o leitor já é um *os.File (caso do multipart grande em disco), passa o caminho
direto ao ffprobe/ffmpeg sem duplicar dezenas ou centenas de MB. Caso contrário, grava
um arquivo temporário e o remove ao terminar. Sempre reposiciona o leitor no início
(Seek(0, io.SeekStart)) para que o upload subsequente leia o arquivo inteiro.
*/
func metadadosDeVideoDeLeitor(r io.ReadSeeker) (segundos, largura, altura uint32, miniatura []byte) {
	if r == nil {
		return 0, 0, 0, nil
	}
	defer func() {
		_, _ = r.Seek(0, io.SeekStart)
	}()

	if f, ok := r.(*os.File); ok {
		return metadadosDeVideo(f.Name())
	}

	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return 0, 0, 0, nil
	}

	tmp, err := os.CreateTemp("", "evo-video-meta-*")
	if err != nil {
		return 0, 0, 0, nil
	}
	nome := tmp.Name()
	defer os.Remove(nome)

	_, copyErr := io.Copy(tmp, r)
	closeErr := tmp.Close()
	if copyErr != nil || closeErr != nil {
		return 0, 0, 0, nil
	}

	return metadadosDeVideo(nome)
}

// metadadosDeVideoDeBytes grava os bytes num temporário (via metadadosDeVideoDeLeitor)
// para o ffprobe/ffmpeg conseguirem ler inclusive MP4 com átomo moov no fim do arquivo.
func metadadosDeVideoDeBytes(dados []byte) (segundos, largura, altura uint32, miniatura []byte) {
	if len(dados) == 0 {
		return 0, 0, 0, nil
	}
	return metadadosDeVideoDeLeitor(bytes.NewReader(dados))
}

func extrairDimensoesEDuracao(caminho string) (segundos, largura, altura uint32) {
	if _, err := exec.LookPath("ffprobe"); err != nil {
		return 0, 0, 0
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeoutMetadadosVideo)
	defer cancel()

	cmd := exec.CommandContext(ctx,
		"ffprobe",
		"-v", "error",
		"-select_streams", "v:0",
		"-show_entries", "stream=width,height:format=duration",
		"-of", "json",
		caminho,
	)
	saida, err := cmd.Output()
	if err != nil {
		return 0, 0, 0
	}

	var resultado struct {
		Streams []struct {
			Width  int `json:"width"`
			Height int `json:"height"`
		} `json:"streams"`
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
	}
	if err := json.Unmarshal(saida, &resultado); err != nil {
		return 0, 0, 0
	}

	if len(resultado.Streams) > 0 {
		if resultado.Streams[0].Width > 0 {
			largura = uint32(resultado.Streams[0].Width)
		}
		if resultado.Streams[0].Height > 0 {
			altura = uint32(resultado.Streams[0].Height)
		}
	}

	if resultado.Format.Duration != "" {
		if dur, err := strconv.ParseFloat(resultado.Format.Duration, 64); err == nil && !math.IsNaN(dur) && !math.IsInf(dur, 0) && dur > 0 {
			// Arredonda para cima para vídeos curtos (< 1 s) não irem com segundos=0.
			segundos = uint32(math.Ceil(dur))
		}
	}

	return segundos, largura, altura
}

func extrairMiniaturaDeVideo(caminho string) []byte {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return nil
	}

	// Pula o primeiro segundo para evitar quadro preto inicial; se o vídeo for mais
	// curto que 1 s e não devolver quadro, tenta novamente a partir do início.
	if img := capturarQuadroJPEG(caminho, true); len(img) > 0 {
		return img
	}
	return capturarQuadroJPEG(caminho, false)
}

func capturarQuadroJPEG(caminho string, pularInicio bool) []byte {
	ctx, cancel := context.WithTimeout(context.Background(), timeoutMetadadosVideo)
	defer cancel()

	args := []string{"-v", "error"}
	if pularInicio {
		args = append(args, "-ss", "1")
	}
	args = append(args,
		"-i", caminho,
		"-frames:v", "1",
		"-vf", "scale=100:-2",
		"-f", "image2",
		"-c:v", "mjpeg",
		"pipe:1",
	)

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil || out.Len() == 0 {
		return nil
	}
	return out.Bytes()
}
