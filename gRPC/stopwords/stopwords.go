package stopwords

import "strings"

// Define um mapa para busca rápida de Stopwords em português.
var PortugueseStopwords = map[string]bool{
	"o": true, "a": true, "os": true, "as": true, "de": true, "da": true, "do": true, "das": true, "dos": true,
	"e": true, "em": true, "para": true, "por": true, "um": true, "uma": true, "uns": true, "umas": true,
	"no": true, "na": true, "nos": true, "nas": true, "ao": true, "aos": true, "à": true, "às": true,
	"se": true, "que": true, "com": true, "como": true, "mais": true, "mas": true, "ou": true, "ser": true,
	"já": true, "não": true, "são": true, "foi": true, "era": true, "é": true, "tem": true, "têm": true,
	"sobre": true, "entre": true, "pela": true, "pelo": true, "pelas": true, "pelos": true, "qual": true,
	"quais": true, "isso": true, "isto": true, "aquele": true, "aquilo": true, "aquela": true, "seu": true,
	"sua": true, "seus": true, "suas": true, "lhe": true, "eles": true, "elas": true, "ele": true, "ela": true,
}

// IsStopword verifica se uma palavra deve ser ignorada.
func IsStopword(word string) bool {
	// Converte para minúsculas e remove espaços antes de verificar
	word = strings.ToLower(strings.TrimSpace(word))
	_, exists := PortugueseStopwords[word]
	return exists
}