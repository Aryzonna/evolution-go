// pkg/call/stream/bridge.go
package call_stream

import (
	"encoding/base64"
	"io"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/purpshell/meowcaller"
)

// wsMessage is the JSON envelope carried over the stream socket, modeled on Twilio
// Media Streams so existing AI-voice integrations need minimal adapting.
type wsMessage struct {
	Event      string `json:"event"`
	CallID     string `json:"callId,omitempty"`
	SampleRate int    `json:"sampleRate,omitempty"`
	Video      bool   `json:"video,omitempty"`
	Track      string `json:"track,omitempty"`
	Payload    string `json:"payload,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

// bridge adapts one WebSocket connection to meowcaller's AudioSink (Call.Receive),
// VideoSink (Call.ReceiveVideo), and AudioSource (Call.Play) interfaces, so a single
// object plugs a call's media straight into the socket in both directions.
type bridge struct {
	conn           *websocket.Conn
	writeMu        sync.Mutex
	incoming       chan []float32
	closed         chan struct{}
	closeOnce      sync.Once
	sendVideo      func([]byte) error
	startVideo     func() error
	startVideoOnce sync.Once

	// Contadores para o balanço da ligação, registrado no Close: sem eles, "um lado não
	// ouve" não diz se o áudio não saiu da meowcaller, não coube no socket ou nunca veio
	// do consumidor.
	callID        string
	inFrames      atomic.Int64 // quadros do celular escritos no socket
	inWriteErrors atomic.Int64 // quadros do celular que o socket recusou
	outFrames     atomic.Int64 // quadros do consumidor entregues à meowcaller
	outDropped    atomic.Int64 // quadros do consumidor descartados com a fila cheia
	startedAt     time.Time

	// Volume do que veio do celular: 352 quadros entregues e ninguém ouvindo não diz se
	// a meowcaller decodificou voz ou silêncio. Medido em 24/11 depois de o balanço de
	// quadros sair inteiro até o navegador.
	levelMu    sync.Mutex
	inAudible  int64   // quadros do celular com RMS acima de audibleRMS
	inPeakRMS  float64 // maior RMS de um quadro do celular
	inRMSSum   float64 // soma dos RMS, para a média
	outAudible int64   // o mesmo para o que o consumidor mandou
	outPeakRMS float64
	outRMSSum  float64
}

// audibleRMS separa fala de silêncio/ruído de fundo num quadro de 60 ms (fala normal
// fica entre 0,02 e 0,2; silêncio digital é 0).
const audibleRMS = 0.01

func frameRMS(frame []float32) float64 {
	if len(frame) == 0 {
		return 0
	}
	var sum float64
	for _, s := range frame {
		sum += float64(s) * float64(s)
	}
	return math.Sqrt(sum / float64(len(frame)))
}

func newBridge(conn *websocket.Conn, callID string) *bridge {
	return &bridge{
		conn:      conn,
		callID:    callID,
		startedAt: time.Now(),
		// ~3 seconds of buffering at one 60ms frame per slot before frames start
		// getting dropped — enough slack for scheduling jitter without unbounded growth.
		incoming: make(chan []float32, 50),
		closed:   make(chan struct{}),
	}
}

func (b *bridge) writeJSON(msg wsMessage) error {
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	return b.conn.WriteJSON(msg)
}

// writeStart sends the initial handshake message once the socket is up.
func (b *bridge) writeStart(callID string, video bool) {
	_ = b.writeJSON(wsMessage{
		Event:      "start",
		CallID:     callID,
		SampleRate: meowcaller.SampleRate,
		Video:      video,
	})
}

// WriteFrame implements meowcaller.AudioSink: one decoded mono frame from the peer.
func (b *bridge) WriteFrame(frame []float32) error {
	rms := frameRMS(frame)
	b.levelMu.Lock()
	b.inRMSSum += rms
	if rms > b.inPeakRMS {
		b.inPeakRMS = rms
	}
	firstAudible := false
	if rms >= audibleRMS {
		b.inAudible++
		firstAudible = b.inAudible == 1
	}
	b.levelMu.Unlock()
	if firstAudible {
		log.Printf("[call-stream] %s: primeiro quadro do celular com som (RMS %.3f)", b.callID, rms)
	}

	payload := base64.StdEncoding.EncodeToString(pcm16FromFloat32(frame))
	err := b.writeJSON(wsMessage{Event: "media", Track: "inbound", Payload: payload})
	if err != nil {
		if b.inWriteErrors.Add(1) == 1 {
			log.Printf("[call-stream] %s: primeiro quadro do celular recusado pelo socket: %v", b.callID, err)
		}
		return err
	}
	if b.inFrames.Add(1) == 1 {
		log.Printf("[call-stream] %s: primeiro quadro do celular enviado ao consumidor (%d amostras)", b.callID, len(frame))
	}
	return nil
}

func mean(sum float64, n int64) float64 {
	if n == 0 {
		return 0
	}
	return sum / float64(n)
}

// WriteVideo implements meowcaller.VideoSink: one Annex-B H.264 access unit.
func (b *bridge) WriteVideo(accessUnit []byte) error {
	payload := base64.StdEncoding.EncodeToString(accessUnit)
	return b.writeJSON(wsMessage{Event: "video", Track: "inbound", Payload: payload})
}

// ReadFrame implements meowcaller.AudioSource: frames the consumer sent back over the
// socket, decoded by readLoop and handed here on demand.
func (b *bridge) ReadFrame() ([]float32, error) {
	select {
	case frame, ok := <-b.incoming:
		if !ok {
			return nil, io.EOF
		}
		return frame, nil
	case <-b.closed:
		return nil, io.EOF
	}
}

// Close satisfies AudioSink/VideoSink/AudioSource's shared Close() error method. Safe
// to call more than once (from the call's OnEnd callback and from readLoop exiting).
func (b *bridge) Close() error {
	b.closeOnce.Do(func() {
		log.Printf("[call-stream] %s: fim após %s — do celular: %d quadros enviados, %d recusados; do consumidor: %d entregues, %d descartados",
			b.callID, time.Since(b.startedAt).Round(time.Second),
			b.inFrames.Load(), b.inWriteErrors.Load(), b.outFrames.Load(), b.outDropped.Load())
		b.levelMu.Lock()
		inTotal, outTotal := b.inFrames.Load()+b.inWriteErrors.Load(), b.outFrames.Load()+b.outDropped.Load()
		log.Printf("[call-stream] %s: volume — do celular: %d de %d quadros com som, média %.4f, pico %.3f; do consumidor: %d de %d com som, média %.4f, pico %.3f",
			b.callID, b.inAudible, inTotal, mean(b.inRMSSum, inTotal), b.inPeakRMS,
			b.outAudible, outTotal, mean(b.outRMSSum, outTotal), b.outPeakRMS)
		b.levelMu.Unlock()
		_ = b.writeJSON(wsMessage{Event: "stop", Reason: "hangup"})
		close(b.closed)
		_ = b.conn.Close()
	})
	return nil
}

// readLoop blocks reading consumer-sent media messages until the connection closes
// (by either side) or send Close(). Every non-media message is ignored rather than
// erroring, so the wire format can grow new event types without breaking old clients.
func (b *bridge) readLoop() {
	var pending []float32
	for {
		var msg wsMessage
		if err := b.conn.ReadJSON(&msg); err != nil {
			b.Close()
			return
		}
		if msg.Event == "video" && msg.Track == "outbound" {
			raw, err := base64.StdEncoding.DecodeString(msg.Payload)
			if err != nil || b.sendVideo == nil {
				continue
			}
			// Spike/experimental: outbound video was explicitly out of scope in the
			// reviewed plan (video is inbound-only there). Added here to test
			// screen-share/camera content actually reaching the peer.
			//
			// meowcaller's StartVideo doc comment: "Outbound video remains gated
			// until the peer acknowledges the transition" -- so the upgrade must be
			// requested before any SendVideo call has an effect. Lazily triggered on
			// the first outbound video message so callers don't need a separate
			// endpoint/step to remember.
			b.startVideoOnce.Do(func() {
				if b.startVideo != nil {
					_ = b.startVideo()
				}
			})
			if err := b.sendVideo(raw); err != nil {
				continue
			}
			continue
		}
		if msg.Event != "media" || msg.Track != "outbound" {
			continue
		}
		raw, err := base64.StdEncoding.DecodeString(msg.Payload)
		if err != nil {
			continue
		}
		// meowcaller's AudioSource contract is exactly FrameSamples (60 ms) per ReadFrame.
		// Consumers send whatever chunk size they capture (the Flow browser sent 20 ms),
		// and short frames made the peer's phone sit on "reconnecting" with no audio.
		// Re-chunk here so any size works.
		pending = append(pending, float32FromPCM16(raw)...)
		for len(pending) >= meowcaller.FrameSamples {
			frame := make([]float32, meowcaller.FrameSamples)
			copy(frame, pending[:meowcaller.FrameSamples])
			pending = pending[meowcaller.FrameSamples:]
			rms := frameRMS(frame)
			b.levelMu.Lock()
			b.outRMSSum += rms
			if rms > b.outPeakRMS {
				b.outPeakRMS = rms
			}
			if rms >= audibleRMS {
				b.outAudible++
			}
			b.levelMu.Unlock()
			select {
			case b.incoming <- frame:
				b.outFrames.Add(1)
			case <-b.closed:
				return
			default:
				// Consumer is sending audio faster than the call can play it out; drop
				// the frame rather than block the socket read loop.
				b.outDropped.Add(1)
			}
		}
	}
}
