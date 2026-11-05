package message_service

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	"github.com/evolution-foundation/evolution-go/pkg/utils"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waMmsRetry"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// PEDIR AO CELULAR QUE REENVIE UMA MÍDIA que o servidor do WhatsApp já apagou.
//
// O servidor de mídia do WhatsApp guarda os arquivos por pouco tempo — medido em
// 04/11/2026: imagem cerca de 30 dias, áudio e vídeo em menos de 3 semanas — e
// depois responde 404/410 (ou 403, quando a URL assinada venceu). O celular
// conectado normalmente ainda tem o arquivo. Pelo protocolo, mandamos a ele um
// "media retry receipt"; ele sobe o arquivo de novo e devolve, cifrado com a
// mediaKey da mensagem, um directPath NOVO. Com ele, o download e a decifragem
// voltam a funcionar como no primeiro dia (a mediaKey não muda).
//
// Devolvemos só o directPath: quem chama já sabe baixar e decifrar, e assim um
// vídeo de centenas de MB não atravessa este processo nem uma resposta JSON.

type MediaRetryStruct struct {
	// ID da mensagem no WhatsApp (stanza ID).
	MessageID string `json:"messageId"`
	// Conversa: número, JID de contato, @lid ou @g.us.
	Chat string `json:"chat"`
	// A mensagem foi enviada por este número?
	FromMe bool `json:"fromMe"`
	// Em grupo, quem enviou a mensagem (JID). Obrigatório quando Chat é @g.us.
	Participant string `json:"participant,omitempty"`
	// mediaKey da mensagem, em base64.
	MediaKey string `json:"mediaKey"`
	// Quanto esperar pelo celular, em segundos (padrão 30, máximo 90).
	TimeoutSeconds int `json:"timeoutSeconds,omitempty"`
}

type MediaRetryResult struct {
	DirectPath string `json:"directPath"`
}

// Motivos que quem chama precisa distinguir para responder à pessoa.
var (
	ErrMediaRetryNotOnPhone = errors.New("o celular não tem mais esta mídia")
	ErrMediaRetryTimeout    = errors.New("o celular não respondeu a tempo (offline ou sem conexão)")
	ErrMediaRetryInvalid    = errors.New("pedido inválido")
)

/*
A RESPOSTA QUE CHEGA DEPOIS DO PRAZO NÃO PODE SE PERDER.

Para responder, o celular sobe o arquivo inteiro de novo: medido em 04/11/2026, um vídeo
respondeu 37 s depois do pedido, com a espera já esgotada — e um segundo pedido para a
mesma mensagem não teve resposta nenhuma. O celular responde uma vez só. Então, quando o
prazo acaba, o ouvinte continua de pé por mais `esperaPelaRespostaAtrasada` e guarda o que
vier aqui; o próximo pedido para a mesma mensagem é atendido na hora, sem incomodar o
celular de novo.

O evento é guardado CIFRADO: quem pede de novo manda a mediaKey, e só com ela se lê.
*/
const (
	esperaPelaRespostaAtrasada = 5 * time.Minute
	validadeDaRespostaGuardada = 10 * time.Minute
)

type respostaGuardada struct {
	evt *events.MediaRetry
	em  time.Time
}

var (
	respostasGuardadas   = map[string]respostaGuardada{}
	respostasGuardadasMu sync.Mutex
)

func chaveDaResposta(instanceID, messageID string) string { return instanceID + "|" + messageID }

func guardarResposta(chave string, evt *events.MediaRetry) {
	respostasGuardadasMu.Lock()
	defer respostasGuardadasMu.Unlock()
	agora := time.Now()
	for k, r := range respostasGuardadas {
		if agora.Sub(r.em) > validadeDaRespostaGuardada {
			delete(respostasGuardadas, k)
		}
	}
	respostasGuardadas[chave] = respostaGuardada{evt: evt, em: agora}
}

func tirarRespostaGuardada(chave string) *events.MediaRetry {
	respostasGuardadasMu.Lock()
	defer respostasGuardadasMu.Unlock()
	r, ok := respostasGuardadas[chave]
	if !ok {
		return nil
	}
	delete(respostasGuardadas, chave)
	if time.Since(r.em) > validadeDaRespostaGuardada {
		return nil
	}
	return r.evt
}

// lerResposta decifra a resposta do celular e a traduz nos erros que o handler conhece.
func lerResposta(evt *events.MediaRetry, mediaKey []byte) (*MediaRetryResult, error) {
	notif, err := whatsmeow.DecryptMediaRetryNotification(evt, mediaKey)
	if errors.Is(err, whatsmeow.ErrMediaNotAvailableOnPhone) {
		return nil, ErrMediaRetryNotOnPhone
	}
	if err != nil {
		return nil, fmt.Errorf("resposta do celular ilegível: %w", err)
	}
	if notif.GetResult() != waMmsRetry.MediaRetryNotification_SUCCESS || notif.GetDirectPath() == "" {
		if notif.GetResult() == waMmsRetry.MediaRetryNotification_NOT_FOUND {
			return nil, ErrMediaRetryNotOnPhone
		}
		return nil, fmt.Errorf("o celular não reenviou a mídia (%s)", notif.GetResult().String())
	}
	return &MediaRetryResult{DirectPath: notif.GetDirectPath()}, nil
}

func (m *messageService) RequestMediaRetry(data *MediaRetryStruct, instance *instance_model.Instance) (*MediaRetryResult, error) {
	if data == nil || strings.TrimSpace(data.MessageID) == "" || strings.TrimSpace(data.Chat) == "" || data.MediaKey == "" {
		return nil, fmt.Errorf("%w: messageId, chat e mediaKey são obrigatórios", ErrMediaRetryInvalid)
	}
	mediaKey, err := base64.StdEncoding.DecodeString(data.MediaKey)
	if err != nil || len(mediaKey) != 32 {
		return nil, fmt.Errorf("%w: mediaKey precisa ser base64 de 32 bytes", ErrMediaRetryInvalid)
	}
	chat, ok := utils.ParseJID(data.Chat)
	if !ok {
		return nil, fmt.Errorf("%w: chat inválido", ErrMediaRetryInvalid)
	}

	info := &types.MessageInfo{
		ID: data.MessageID,
		MessageSource: types.MessageSource{
			Chat:     chat,
			IsFromMe: data.FromMe,
			IsGroup:  chat.Server == types.GroupServer,
		},
	}
	if info.IsGroup {
		sender, ok := utils.ParseJID(data.Participant)
		if !ok {
			return nil, fmt.Errorf("%w: participant é obrigatório em grupo", ErrMediaRetryInvalid)
		}
		info.Sender = sender
	}

	logger := m.loggerWrapper.GetLogger(instance.Id)
	chave := chaveDaResposta(instance.Id, data.MessageID)

	/* Uma resposta que chegou depois do prazo de um pedido anterior atende este na hora. */
	if guardada := tirarRespostaGuardada(chave); guardada != nil {
		logger.LogInfo("[%s] media retry: %s atendido pela resposta guardada do celular", instance.Id, data.MessageID)
		return lerResposta(guardada, mediaKey)
	}

	client, err := m.ensureClientConnected(instance.Id)
	if err != nil {
		return nil, err
	}

	timeout := 30 * time.Second
	if data.TimeoutSeconds > 0 {
		timeout = time.Duration(min(data.TimeoutSeconds, 90)) * time.Second
	}

	/*
		O OUVINTE É REGISTRADO ANTES DO PEDIDO. A resposta do celular pode chegar em
		milissegundos; registrado depois do envio, o evento passaria sem ninguém para
		recebê-lo. O canal tem buffer de 1 e o envio não bloqueia: o tratador de eventos
		do whatsmeow não pode ficar preso esperando por nós.
	*/
	resposta := make(chan *events.MediaRetry, 1)
	ouvinte := client.AddEventHandler(func(evt any) {
		if r, ok := evt.(*events.MediaRetry); ok && r.MessageID == data.MessageID {
			select {
			case resposta <- r:
			default:
			}
		}
	})
	removido := false
	defer func() {
		if !removido {
			client.RemoveEventHandler(ouvinte)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := client.SendMediaRetryReceipt(ctx, info, mediaKey); err != nil {
		logger.LogError("[%s] media retry: falha ao enviar o pedido de %s: %v", instance.Id, data.MessageID, err)
		return nil, fmt.Errorf("falha ao pedir a mídia ao celular: %w", err)
	}
	logger.LogInfo("[%s] media retry: pedido enviado ao celular para %s", instance.Id, data.MessageID)

	select {
	case evt := <-resposta:
		resultado, err := lerResposta(evt, mediaKey)
		if err != nil {
			logger.LogInfo("[%s] media retry: %s — %v", instance.Id, data.MessageID, err)
			return nil, err
		}
		logger.LogInfo("[%s] media retry: %s reenviada pelo celular", instance.Id, data.MessageID)
		return resultado, nil
	case <-ctx.Done():
		logger.LogInfo("[%s] media retry: sem resposta do celular para %s em %s; guardando a resposta se ela vier", instance.Id, data.MessageID, timeout)
		removido = true
		go func() {
			defer client.RemoveEventHandler(ouvinte)
			select {
			case evt := <-resposta:
				guardarResposta(chave, evt)
				logger.LogInfo("[%s] media retry: resposta atrasada de %s guardada", instance.Id, data.MessageID)
			case <-time.After(esperaPelaRespostaAtrasada):
			}
		}()
		return nil, ErrMediaRetryTimeout
	}
}
