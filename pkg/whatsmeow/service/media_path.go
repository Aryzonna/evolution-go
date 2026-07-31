package whatsmeow_service

import "strings"

// maxInstanceSegmentLength limita o segmento da instância dentro da chave do objeto.
// Um id de instância é um UUID (36 caracteres); o teto só existe para que um valor
// absurdo não gere uma chave gigante no bucket.
const maxInstanceSegmentLength = 96

// buildMediaFileName monta o nome do arquivo de mídia já com o segmento da instância.
//
// O Store do MinIO apenas concatena o prefixo fixo "evolution-go-medias/" ao nome
// recebido, então devolver "<instância>/<arquivo>" aqui basta para o objeto final ficar
// em "evolution-go-medias/<instanceId>/<messageId>.<ext>" em vez de
// "evolution-go-medias/<messageId>.<ext>". Nenhuma assinatura de interface muda.
//
// Se o id da instância não sobreviver à sanitização (vazio, só separadores, etc.),
// devolvemos o nome puro e a gravação volta ao formato antigo, plano. Perder a separação
// por instância é melhor do que gravar numa chave malformada e perder a mídia.
func buildMediaFileName(instanceID string, fileName string) string {
	segment := sanitizeInstanceSegment(instanceID)
	if segment == "" {
		return fileName
	}

	return segment + "/" + fileName
}

// sanitizeInstanceSegment reduz o id da instância a UM segmento seguro de chave de objeto:
// apenas letras, dígitos, '-' e '_'. Qualquer outro caractere (barra, ponto, espaço,
// acento, controle) vira '_', o que elimina de vez "/" e ".." do caminho e garante que o
// segmento não se parta em dois níveis.
//
// Devolve "" quando não sobra nenhum caractere alfanumérico — nesse caso o chamador cai no
// formato antigo em vez de gravar em "evolution-go-medias/___/arquivo.jpg".
func sanitizeInstanceSegment(instanceID string) string {
	trimmed := strings.TrimSpace(instanceID)
	if trimmed == "" {
		return ""
	}

	runes := []rune(trimmed)
	if len(runes) > maxInstanceSegmentLength {
		runes = runes[:maxInstanceSegmentLength]
	}

	var builder strings.Builder
	builder.Grow(len(runes))

	hasAlphanumeric := false
	for _, r := range runes {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			hasAlphanumeric = true
			builder.WriteRune(r)
		case r == '-', r == '_':
			builder.WriteRune(r)
		default:
			builder.WriteByte('_')
		}
	}

	if !hasAlphanumeric {
		return ""
	}

	return builder.String()
}
