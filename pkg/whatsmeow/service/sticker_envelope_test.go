package whatsmeow_service

import (
	"testing"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Monta os bytes como o WhatsApp manda a figurinha com som e os decodifica com
// a whatsmeow que usamos: o envelope (134) e o áudio (26) viram campos
// desconhecidos, que é exatamente a situação de produção.
func TestStickerFromEnvelopes_AudioSticker(t *testing.T) {
	stickerBytes := mustMarshal(t, &waE2E.StickerMessage{
		URL:        proto.String("https://mmg.whatsapp.net/sticker"),
		Mimetype:   proto.String("image/webp"),
		IsAnimated: proto.Bool(true),
	})
	audioBytes := mustMarshal(t, &waE2E.AudioMessage{
		URL:      proto.String("https://mmg.whatsapp.net/audio"),
		Mimetype: proto.String("audio/ogg; codecs=opus"),
	})
	stickerBytes = protowire.AppendTag(stickerBytes, stickerAudioMessageField, protowire.BytesType)
	stickerBytes = protowire.AppendBytes(stickerBytes, audioBytes)

	// Message{stickerMessage = 26} dentro do FutureProofMessage{message = 1}.
	inner := protowire.AppendTag(nil, 26, protowire.BytesType)
	inner = protowire.AppendBytes(inner, stickerBytes)
	envelope := protowire.AppendTag(nil, 1, protowire.BytesType)
	envelope = protowire.AppendBytes(envelope, inner)

	raw := mustMarshal(t, &waE2E.Message{MessageContextInfo: &waE2E.MessageContextInfo{}})
	raw = protowire.AppendTag(raw, audioStickerMessageField, protowire.BytesType)
	raw = protowire.AppendBytes(raw, envelope)

	msg := &waE2E.Message{}
	if err := proto.Unmarshal(raw, msg); err != nil {
		t.Fatal(err)
	}
	if msg.GetStickerMessage() != nil {
		t.Fatal("premissa quebrada: a figurinha com som não deveria aparecer como stickerMessage direto")
	}

	sticker, audio := stickerFromEnvelopes(msg)
	if sticker.GetURL() != "https://mmg.whatsapp.net/sticker" || !sticker.GetIsAnimated() {
		t.Fatalf("figurinha não saiu do envelope: %v", sticker)
	}
	if audio.GetURL() != "https://mmg.whatsapp.net/audio" {
		t.Fatalf("áudio da figurinha não saiu: %v", audio)
	}
}

func TestStickerFromEnvelopes_AudioStickerSemSom(t *testing.T) {
	inner := mustMarshal(t, &waE2E.Message{StickerMessage: &waE2E.StickerMessage{URL: proto.String("u")}})
	envelope := protowire.AppendTag(nil, 1, protowire.BytesType)
	envelope = protowire.AppendBytes(envelope, inner)
	raw := protowire.AppendTag(nil, audioStickerMessageField, protowire.BytesType)
	raw = protowire.AppendBytes(raw, envelope)

	msg := &waE2E.Message{}
	if err := proto.Unmarshal(raw, msg); err != nil {
		t.Fatal(err)
	}
	sticker, audio := stickerFromEnvelopes(msg)
	if sticker.GetURL() != "u" || audio != nil {
		t.Fatalf("esperava figurinha sem áudio, veio %v / %v", sticker, audio)
	}
}

func TestStickerFromEnvelopes_Lottie(t *testing.T) {
	msg := &waE2E.Message{LottieStickerMessage: &waE2E.FutureProofMessage{
		Message: &waE2E.Message{StickerMessage: &waE2E.StickerMessage{
			URL:      proto.String("lottie"),
			IsLottie: proto.Bool(true),
		}},
	}}
	sticker, audio := stickerFromEnvelopes(msg)
	if sticker.GetURL() != "lottie" || audio != nil {
		t.Fatalf("Lottie não saiu do envelope: %v / %v", sticker, audio)
	}
}

func TestStickerFromEnvelopes_SemEnvelope(t *testing.T) {
	sticker, audio := stickerFromEnvelopes(&waE2E.Message{Conversation: proto.String("oi")})
	if sticker != nil || audio != nil {
		t.Fatal("mensagem de texto não deveria virar figurinha")
	}
	if s, a := stickerFromEnvelopes(nil); s != nil || a != nil {
		t.Fatal("nil deveria devolver nil")
	}
}
