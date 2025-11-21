package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/segmentio/kafka-go"
)

var kafkaBrokers = []string{
	"localhost:29092",
	"localhost:29093",
	"localhost:29094",
	"localhost:29095",
}

const (
	DividedTopic        = "divided_texts"
	PartialResultsTopic = "partial_word_counts"
	MaxWordsInResult    = 50 // limite topo
)

// conjunto de stopwords PT-BR (simplificado)
var stopwords = map[string]struct{}{
	"o": {}, "a": {}, "os": {}, "as": {}, "de": {}, "da": {}, "do": {}, "das": {}, "dos": {},
	"e": {}, "em": {}, "para": {}, "por": {}, "um": {}, "uma": {}, "uns": {}, "umas": {},
	"no": {}, "na": {}, "nos": {}, "nas": {}, "ao": {}, "aos": {}, "à": {}, "às": {},
	"se": {}, "que": {}, "com": {}, "como": {}, "mais": {}, "mas": {}, "ou": {}, "ser": {},
	"já": {}, "não": {}, "são": {}, "foi": {}, "era": {}, "é": {}, "tem": {}, "têm": {},
	"sobre": {}, "entre": {}, "pela": {}, "pelo": {}, "pelas": {}, "pelos": {}, "qual": {},
	"quais": {}, "isso": {}, "isto": {}, "aquele": {}, "aquilo": {}, "aquela": {}, "seu": {},
	"sua": {}, "seus": {}, "suas": {}, "lhe": {}, "eles": {}, "elas": {}, "ele": {}, "ela": {},
}

func newChunkReader() *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  kafkaBrokers,
		Topic:    DividedTopic,
		GroupID:  "word-worker",
		MinBytes: 1,
		MaxBytes: 5e6,
	})
}

func newResultWriter() *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:  kafkaBrokers,
		Topic:    PartialResultsTopic,
		Balancer: &kafka.LeastBytes{},
	})
}

func isSeparator(r rune) bool {
	// Considera tudo que não é letra ou número como separador
	return !unicode.IsLetter(r) && !unicode.IsNumber(r)
}

func tokenize(text string) []string {
	// Usa strings.FieldsFunc que é mais eficiente que regex para este caso.
	words := strings.FieldsFunc(strings.ToLower(text), isSeparator)

	tokens := make([]string, 0, len(words))
	for _, w := range words {
		if len(w) < 2 {
			continue
		}
		if _, stop := stopwords[w]; stop {
			continue
		}
		tokens = append(tokens, w)
	}
	return tokens
}

func processChunk(raw string) map[string]int {
	tokens := tokenize(raw)
	freq := make(map[string]int, len(tokens))
	for _, t := range tokens {
		freq[t]++
	}
	return freq
}

func main() {
	reader := newChunkReader()
	defer reader.Close()
	writer := newResultWriter()
	defer writer.Close()

	log.Printf("Worker aguardando chunks em '%s' -> resultados em '%s'", DividedTopic, PartialResultsTopic)

	ctx := context.Background()
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("Erro lendo chunk: %v", err)
			time.Sleep(time.Second)
			continue
		}

		var srcKey string
		var chunkIndex, chunkTotal int
		for _, h := range msg.Headers {
			switch h.Key {
			case "source-key":
				srcKey = string(h.Value)
			case "chunk-index":
				if v, e := strconv.Atoi(string(h.Value)); e == nil {
					chunkIndex = v
				}
			case "chunk-total":
				if v, e := strconv.Atoi(string(h.Value)); e == nil {
					chunkTotal = v
				}
			}
		}
		if srcKey == "" {
			srcKey = string(msg.Key)
		}

		res := processChunk(string(msg.Value))
		payload, err := json.Marshal(res)
		if err != nil {
			log.Printf("Falha serializando resultado chunk %d: %v", chunkIndex, err)
			continue
		}

		err = writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(srcKey + "-chunk-" + strconv.Itoa(chunkIndex)),
			Value: payload,
			Headers: []kafka.Header{
				{Key: "source-key", Value: []byte(srcKey)},
				{Key: "chunk-index", Value: []byte(strconv.Itoa(chunkIndex))},
				{Key: "chunk-total", Value: []byte(strconv.Itoa(chunkTotal))},
			},
		})
		if err != nil {
			log.Printf("Erro enviando resultado chunk %d: %v", chunkIndex, err)
			continue
		}
		log.Printf("Resultado enviado chunk %d/%d (%s) palavras=%d", chunkIndex+1, chunkTotal, srcKey, len(res))
	}
}