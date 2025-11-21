package stopwords

import "strings"

// Define um mapa para busca rápida de Stopwords em português.
var PortugueseStopwords = map[string]bool{
	"a": true, "o": true, "as": true, "os": true, "e": true, "ou": true,
	"de": true, "do": true, "da": true, "dos": true, "das": true, "em": true,
	"no": true, "na": true, "nos": true, "nas": true, "um": true, "uma": true,
	"uns": true, "umas": true, "para": true, "por": true, "com": true, "se": true,
	"que": true, "é": true, "são": true, "como": true, "mais": true, "mas": true,
	"este": true, "esta": true, "isto": true, "isso": true, "aquele": true, "aquela": true,
}

// IsStopword verifica se uma palavra deve ser ignorada.
func IsStopword(word string) bool {
	// Converte para minúsculas e remove espaços antes de verificar
	word = strings.ToLower(strings.TrimSpace(word))
	_, exists := PortugueseStopwords[word]
	return exists
}