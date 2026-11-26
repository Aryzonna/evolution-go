package whatsmeow_service

import (
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Figurinha animada COM SOM (recurso novo do WhatsApp). Ela chega num envelope
// próprio, Message.audioStickerMessage (campo 134, um FutureProofMessage), e o
// áudio vai dentro do StickerMessage, no oneof `audio` (campo 26, AudioMessage).
// A whatsmeow que usamos (2026-06-30) ainda não conhece nenhum dos dois: o
// protobuf os guarda como bytes desconhecidos e nenhum getter os alcança — por
// isso o download para o bucket nunca acontecia e o Flow recebia a figurinha
// sem mediaUrl. Os números saem do .proto da whatsmeow upstream de 2026-11-26.
const (
	audioStickerMessageField protowire.Number = 134
	stickerAudioMessageField protowire.Number = 26
)

// stickerFromEnvelopes devolve a figurinha que vem embrulhada num envelope
// (com som, campo 134; ou Lottie, campo 74) e, na com som, o áudio que a
// acompanha. Mensagem sem envelope devolve nil, nil.
func stickerFromEnvelopes(msg *waE2E.Message) (*waE2E.StickerMessage, *waE2E.AudioMessage) {
	if msg == nil {
		return nil, nil
	}

	envelope := &waE2E.FutureProofMessage{}
	if readMessageField(msg, audioStickerMessageField, envelope) {
		if sticker := envelope.GetMessage().GetStickerMessage(); sticker != nil {
			audio := &waE2E.AudioMessage{}
			if !readMessageField(sticker, stickerAudioMessageField, audio) {
				audio = nil
			}
			return sticker, audio
		}
	}

	if sticker := msg.GetLottieStickerMessage().GetMessage().GetStickerMessage(); sticker != nil {
		return sticker, nil
	}

	return nil, nil
}

// readMessageField preenche `out` com o submensagem de número `num` de `m`.
// Se a lib já conhece o campo, lê pelo descritor; senão, garimpa os bytes
// desconhecidos — assim continua certo quando a whatsmeow for atualizada e o
// campo deixar de ser desconhecido.
func readMessageField(m proto.Message, num protowire.Number, out proto.Message) bool {
	ref := m.ProtoReflect()

	if fd := ref.Descriptor().Fields().ByNumber(num); fd != nil {
		if fd.Kind() != protoreflect.MessageKind || fd.IsList() || fd.IsMap() || !ref.Has(fd) {
			return false
		}
		raw, err := proto.Marshal(ref.Get(fd).Message().Interface())
		return err == nil && proto.Unmarshal(raw, out) == nil
	}

	// Como no protobuf, ocorrências repetidas do mesmo campo se fundem.
	found := false
	unknown := ref.GetUnknown()
	for len(unknown) > 0 {
		n, typ, tagLen := protowire.ConsumeTag(unknown)
		if tagLen < 0 {
			return false
		}
		valLen := protowire.ConsumeFieldValue(n, typ, unknown[tagLen:])
		if valLen < 0 {
			return false
		}
		if n == num && typ == protowire.BytesType {
			value, _ := protowire.ConsumeBytes(unknown[tagLen:])
			if err := (proto.UnmarshalOptions{Merge: true}).Unmarshal(value, out); err != nil {
				return false
			}
			found = true
		}
		unknown = unknown[tagLen+valLen:]
	}
	return found
}
