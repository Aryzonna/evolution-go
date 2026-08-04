package whatsmeow_service

import (
	"encoding/json"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

// Gêmeo de TestArchiveEventPayload, e pela mesma razão: postMap["data"] nasce como
// o evento CRU, então o case Pin monta o mapa à mão em vez de fazer type assertion
// para map[string]interface{} — que estouraria e faria o evento morrer no recover
// do whatsmeow, sem chegar ao webhook nem à fila.
//
// O ponto mais delicado está no fim: `pinned` é *bool com `omitempty`. Ponteiro
// para false NÃO é vazio para o encoding/json, então o desafixar viaja explícito —
// se um dia isso mudar, o handler TS leria "indefinido" e nunca desafixaria nada.
func TestPinEventPayload(t *testing.T) {
	jid, _ := types.ParseJID("554591278430@s.whatsapp.net")
	evt := &events.Pin{
		JID:          jid,
		Timestamp:    time.Now(),
		Action:       &waSyncAction.PinAction{Pinned: proto.Bool(true)},
		FromFullSync: false,
	}

	// ── o estado inicial de myEventHandler ──
	postMap := make(map[string]interface{})
	postMap["data"] = interface{}(evt)

	// ── a asserção direta que derrubaria o evento ──
	if _, ok := postMap["data"].(map[string]interface{}); ok {
		t.Fatal("premissa quebrada: postMap[\"data\"] deveria ser o evento cru, não um mapa")
	}

	// ── mesma construção do case Pin ──
	postMap["data"] = map[string]interface{}{
		"JID":          evt.JID,
		"Timestamp":    evt.Timestamp,
		"Action":       evt.Action,
		"FromFullSync": evt.FromFullSync,
	}

	raw, err := json.Marshal(postMap)
	if err != nil {
		t.Fatalf("marshal falhou: %v", err)
	}

	// ── o formato que o handler TS lê (processPinEvent) ──
	var out struct {
		Data struct {
			JID          string `json:"JID"`
			FromFullSync bool   `json:"FromFullSync"`
			Action       struct {
				Pinned *bool `json:"pinned"`
			} `json:"Action"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal falhou: %v", err)
	}

	if out.Data.JID != "554591278430@s.whatsapp.net" {
		t.Errorf("JID deve serializar como string; veio %q", out.Data.JID)
	}
	if out.Data.Action.Pinned == nil || !*out.Data.Action.Pinned {
		t.Errorf("Action.pinned deveria ser true; veio %v", out.Data.Action.Pinned)
	}
	if out.Data.FromFullSync {
		t.Error("FromFullSync deveria ser false")
	}

	// ── desafixar precisa chegar como false EXPLÍCITO (não omitido) ──
	postMap["data"] = map[string]interface{}{
		"JID":          evt.JID,
		"Action":       &waSyncAction.PinAction{Pinned: proto.Bool(false)},
		"FromFullSync": false,
	}
	raw, _ = json.Marshal(postMap)
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal (unpin) falhou: %v", err)
	}
	if out.Data.Action.Pinned == nil {
		t.Error("pinned=false não pode ser omitido do JSON — o handler leria como indefinido")
	} else if *out.Data.Action.Pinned {
		t.Error("pinned deveria ser false no desafixamento")
	}
}
