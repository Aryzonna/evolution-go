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

// Reproduz o bug que matava o arquivamento vindo do celular: postMap["data"]
// nasce como o evento CRU, e o case Archive fazia um type assertion direto para
// map[string]interface{} — panic, evento descartado pelo recover do whatsmeow,
// nada chegava ao webhook nem à fila.
func TestArchiveEventPayload(t *testing.T) {
	jid, _ := types.ParseJID("554591278430@s.whatsapp.net")
	evt := &events.Archive{
		JID:          jid,
		Timestamp:    time.Now(),
		Action:       &waSyncAction.ArchiveChatAction{Archived: proto.Bool(true)},
		FromFullSync: false,
	}

	// ── o estado inicial de myEventHandler ──
	postMap := make(map[string]interface{})
	postMap["data"] = interface{}(evt)

	// ── o código ANTIGO estourava aqui ──
	if _, ok := postMap["data"].(map[string]interface{}); ok {
		t.Fatal("premissa quebrada: postMap[\"data\"] deveria ser o evento cru, não um mapa")
	}

	// ── o código NOVO (mesma construção do case Archive) ──
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

	// ── o formato que o handler TS lê (processArchiveEvent) ──
	var out struct {
		Data struct {
			JID          string `json:"JID"`
			FromFullSync bool   `json:"FromFullSync"`
			Action       struct {
				Archived *bool `json:"archived"`
			} `json:"Action"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal falhou: %v", err)
	}

	if out.Data.JID != "554591278430@s.whatsapp.net" {
		t.Errorf("JID deve serializar como string; veio %q", out.Data.JID)
	}
	if out.Data.Action.Archived == nil || !*out.Data.Action.Archived {
		t.Errorf("Action.archived deveria ser true; veio %v", out.Data.Action.Archived)
	}
	if out.Data.FromFullSync {
		t.Error("FromFullSync deveria ser false")
	}

	// ── desarquivar precisa chegar como false EXPLÍCITO (não omitido) ──
	postMap["data"] = map[string]interface{}{
		"JID":          evt.JID,
		"Action":       &waSyncAction.ArchiveChatAction{Archived: proto.Bool(false)},
		"FromFullSync": false,
	}
	raw, _ = json.Marshal(postMap)
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal (unarchive) falhou: %v", err)
	}
	if out.Data.Action.Archived == nil {
		t.Error("archived=false não pode ser omitido do JSON — o handler leria como indefinido")
	} else if *out.Data.Action.Archived {
		t.Error("archived deveria ser false no desarquivamento")
	}
}
