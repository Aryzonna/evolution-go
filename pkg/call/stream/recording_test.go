// pkg/call/stream/recording_test.go
package call_stream

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/purpshell/meowcaller"
)

func withRecordPaths(t *testing.T, switchOn bool) string {
	t.Helper()
	dir := t.TempDir()
	oldSwitch, oldDir := recordSwitchPath, recordDir
	recordSwitchPath = filepath.Join(dir, "gravar-ligacao")
	recordDir = filepath.Join(dir, "gravadas")
	t.Cleanup(func() { recordSwitchPath, recordDir = oldSwitch, oldDir })
	if switchOn {
		if err := os.WriteFile(recordSwitchPath, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return recordDir
}

func TestRecorderOffWithoutSwitch(t *testing.T) {
	withRecordPaths(t, false)
	if r := newRecorder("ABC"); r != nil {
		t.Fatal("gravou sem o arquivo-chave")
	}
	var r *recorder
	r.add(make([]float32, 960)) // nil não pode quebrar a ponte
	r.finish()
}

func TestRecorderWritesWavCappedAt20s(t *testing.T) {
	dir := withRecordPaths(t, true)
	r := newRecorder("F640/../x")
	frame := make([]float32, meowcaller.FrameSamples)
	for i := range frame {
		frame[i] = 0.25
	}
	for i := 0; i < 400; i++ { // 24 s de quadros de 60 ms
		r.add(frame)
	}
	r.finish()

	data, err := os.ReadFile(filepath.Join(dir, "F640x-celular.wav"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data[0:4]) != "RIFF" || string(data[8:12]) != "WAVE" || string(data[36:40]) != "data" {
		t.Fatal("cabeçalho WAV inválido")
	}
	if got := binary.LittleEndian.Uint32(data[24:]); got != meowcaller.SampleRate {
		t.Fatalf("taxa %d", got)
	}
	if got := int(binary.LittleEndian.Uint32(data[40:])); got != recordMaxSamples*2 || len(data) != 44+got {
		t.Fatalf("tamanho de dados %d, arquivo %d", got, len(data))
	}
}
