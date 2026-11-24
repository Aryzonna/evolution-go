// pkg/call/stream/recording.go
package call_stream

import (
	"encoding/binary"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/purpshell/meowcaller"
)

// Gravação de diagnóstico do áudio que vem do celular: os primeiros segundos de cada
// ligação viram um WAV, para OUVIR se a meowcaller decodificou voz ou silêncio.
//
// Só liga quando o arquivo-chave existe no contêiner no momento em que a ligação abre
// (`touch /tmp/gravar-ligacao`), então nenhuma ligação é gravada sem alguém pedir, e não
// há variável de ambiente nem nova publicação para ligar e desligar. Tudo fica em /tmp:
// some quando o contêiner é recriado. Criado em 24/11 para a ligação que o Flow não ouvia.
var (
	recordSwitchPath = "/tmp/gravar-ligacao"
	recordDir        = "/tmp/ligacoes-gravadas"
)

// recordMaxSamples limita a gravação a 20 s: o bastante para ouvir a pessoa falar, sem
// guardar a ligação inteira.
const recordMaxSamples = 20 * meowcaller.SampleRate

type recorder struct {
	mu   sync.Mutex
	pcm  []byte
	path string
	done bool
}

// newRecorder devolve nil (sem gravar) quando o arquivo-chave não existe.
func newRecorder(callID string) *recorder {
	if _, err := os.Stat(recordSwitchPath); err != nil {
		return nil
	}
	safe := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return -1
	}, callID)
	if safe == "" {
		safe = "sem-id"
	}
	log.Printf("[call-stream] %s: gravação de diagnóstico ligada (até %d s do celular)", callID, recordMaxSamples/meowcaller.SampleRate)
	return &recorder{
		path: filepath.Join(recordDir, safe+"-celular.wav"),
		pcm:  make([]byte, 0, recordMaxSamples*2),
	}
}

func (r *recorder) add(frame []float32) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done {
		return
	}
	r.pcm = append(r.pcm, pcm16FromFloat32(frame)...)
	if len(r.pcm) >= recordMaxSamples*2 {
		r.pcm = r.pcm[:recordMaxSamples*2]
		r.flushLocked()
	}
}

func (r *recorder) finish() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.flushLocked()
}

func (r *recorder) flushLocked() {
	if r.done {
		return
	}
	r.done = true
	if len(r.pcm) == 0 {
		log.Printf("[call-stream] gravação %s: nenhum áudio do celular chegou", r.path)
		return
	}
	if err := os.MkdirAll(recordDir, 0o700); err != nil {
		log.Printf("[call-stream] gravação %s: não foi possível criar a pasta: %v", r.path, err)
		return
	}
	if err := os.WriteFile(r.path, wavMono16(r.pcm, meowcaller.SampleRate), 0o600); err != nil {
		log.Printf("[call-stream] gravação %s: não foi possível gravar: %v", r.path, err)
		return
	}
	log.Printf("[call-stream] gravação salva em %s (%.1f s)", r.path, float64(len(r.pcm)/2)/meowcaller.SampleRate)
	r.pcm = nil
}

// wavMono16 embrulha PCM16 little-endian mono num cabeçalho WAV (RIFF) de 44 bytes.
func wavMono16(pcm []byte, sampleRate int) []byte {
	out := make([]byte, 44+len(pcm))
	copy(out[0:], "RIFF")
	binary.LittleEndian.PutUint32(out[4:], uint32(36+len(pcm)))
	copy(out[8:], "WAVEfmt ")
	binary.LittleEndian.PutUint32(out[16:], 16)                   // tamanho do bloco fmt
	binary.LittleEndian.PutUint16(out[20:], 1)                    // PCM
	binary.LittleEndian.PutUint16(out[22:], 1)                    // mono
	binary.LittleEndian.PutUint32(out[24:], uint32(sampleRate))   // amostras por segundo
	binary.LittleEndian.PutUint32(out[28:], uint32(sampleRate*2)) // bytes por segundo
	binary.LittleEndian.PutUint16(out[32:], 2)                    // bytes por amostra
	binary.LittleEndian.PutUint16(out[34:], 16)                   // bits por amostra
	copy(out[36:], "data")
	binary.LittleEndian.PutUint32(out[40:], uint32(len(pcm)))
	copy(out[44:], pcm)
	return out
}
