package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"strconv"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

var kafkaBrokers = []string{
	"localhost:29092",
	"localhost:29093",
	"localhost:29094",
	"localhost:29095",
}

const (
	Topic            = "text-input"
	DividedTopic     = "divided_texts"
	ResultTopic      = "partial_word_counts"
	FinalResultTopic = "results"
	ChunkSizeWords   = 100
	TopNWords        = 10
	KafkaWriteMaxRetries  = 5
	KafkaWriteBaseBackoff = 150 * time.Millisecond
)

type ConsolidationState struct {
	TotalChunks    int
	ReceivedChunks int
	WordCounts     map[string]int
	StartTime      time.Time
}

type WordCount struct {
	Word  string
	Count int
}

var (
	consolidationMap = make(map[string]*ConsolidationState)
	mapMutex         = &sync.Mutex{}
)

func newKafkaWriter() *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:  kafkaBrokers,
		Topic:    DividedTopic,
		Balancer: &kafka.LeastBytes{},
	})
}

func newKafkaWriterForTopic(topic string) *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:      kafkaBrokers,
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: int(kafka.RequireAll), // força confirmação de líder
	})
}

func chunkWords(text string, n int) []string {
	words := strings.Fields(text)
	if len(words) == 0 {
		return []string{}
	}
	var chunks []string
	for i := 0; i < len(words); i += n {
		end := i + n
		if end > len(words) {
			end = len(words)
		}
		chunks = append(chunks, strings.Join(words[i:end], " "))
	}
	return chunks
}

func newKafkaReader(topic string, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        kafkaBrokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0, // Desabilita auto-commit
	})
}

func getHeaderValue(headers []kafka.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func isNotLeaderErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "Not Leader For Partition")
}

// writeWithRetry tenta enviar mensagens recriando writer se erro de liderança ocorrer.
func writeWithRetry(ctx context.Context, makeWriter func() *kafka.Writer, w **kafka.Writer, msgs []kafka.Message) error {
	var lastErr error
	for attempt := 1; attempt <= KafkaWriteMaxRetries; attempt++ {
		if *w == nil {
			*w = makeWriter()
		}
		lastErr = (*w).WriteMessages(ctx, msgs...)
		if lastErr == nil {
			return nil
		}
		if isNotLeaderErr(lastErr) {
			(*w).Close()
			*w = nil // força recriação
		}
		time.Sleep(KafkaWriteBaseBackoff * time.Duration(attempt))
	}
	return errors.New("write failed after retries: " + lastErr.Error())
}

/*
Correlacionamento:
Cada texto original recebe um task_id (a própria Kafka message key ou um UUID gerado se vazio).
Esse task_id é colocado em todos os chunks (header: source-key).
Os workers devolvem contagens parciais no tópico partial_word_counts mantendo o header source-key.
O consolidator só acumula resultados dentro de consolidationMap[source-key], impedindo mistura entre tarefas.
Quando ReceivedChunks == TotalChunks a tarefa é finalizada e removida.
*/

func runChunker(wg *sync.WaitGroup) {
	defer wg.Done()
	r := newKafkaReader(Topic, "chunker-group")
	defer r.Close()
	writer := newKafkaWriterForTopic(DividedTopic)
	defer func() { if writer != nil { writer.Close() } }()

	log.Println("Iniciando leitura do tópico:", Topic)

	for {
		m, err := r.FetchMessage(context.Background())
		if err != nil {
			log.Printf("Erro ao buscar mensagem de %s: %v", Topic, err)
			continue
		}

		origKey := string(m.Key)
		if len(origKey) == 0 {
			origKey = uuid.NewString() // garante unicidade para correlação
		}
		texto := string(m.Value)
		chunks := chunkWords(texto, ChunkSizeWords)
		total := len(chunks)
		if total == 0 {
			log.Printf("Mensagem vazia key=%s ignorada.", origKey)
			if err := r.CommitMessages(context.Background(), m); err != nil {
				log.Printf("Falha ao commitar mensagem vazia: %v", err)
			}
			continue
		}

		mapMutex.Lock()
		consolidationMap[origKey] = &ConsolidationState{
			TotalChunks:    total,
			ReceivedChunks: 0,
			WordCounts:     make(map[string]int),
			StartTime:      time.Now(),
		}
		mapMutex.Unlock()

		log.Printf("Recebido key=%s palavras=%d gerando %d chunks => tópico '%s'",
			origKey, len(strings.Fields(texto)), total, DividedTopic)

		var messagesToSend []kafka.Message
		for i, c := range chunks {
			chunkKey := origKey + "-chunk-" + strconv.Itoa(i)
			messagesToSend = append(messagesToSend, kafka.Message{
				Key:   []byte(chunkKey),
				Value: []byte(c),
				Headers: []kafka.Header{
					{Key: "source-key", Value: []byte(origKey)},
					{Key: "chunk-index", Value: []byte(strconv.Itoa(i))},
					{Key: "chunk-total", Value: []byte(strconv.Itoa(total))},
				},
			})
		}

		err = writeWithRetry(context.Background(), func() *kafka.Writer { return newKafkaWriterForTopic(DividedTopic) }, &writer, messagesToSend)
		if err != nil {
			log.Printf("Falha enviando chunks para key=%s após retries: %v", origKey, err)
			// Não faz o commit, para reprocessar a mensagem
			continue
		}

		for i, c := range chunks {
			log.Printf("Chunk %d/%d enviado key=%s-chunk-%d tamanho=%d", i+1, total, origKey, i, len(c))
		}

		if err := r.CommitMessages(context.Background(), m); err != nil {
			log.Printf("Falha ao commitar mensagem: %v", err)
		}
	}
}

func runConsolidator(wg *sync.WaitGroup) {
	defer wg.Done()
	r := newKafkaReader(ResultTopic, "consolidator-group")
	defer r.Close()

	resultWriter := newKafkaWriterForTopic(FinalResultTopic)
	defer func() { if resultWriter != nil { resultWriter.Close() } }()

	log.Println("Iniciando leitura do tópico de resultados:", ResultTopic)

	for {
		m, err := r.FetchMessage(context.Background())
		if err != nil {
			log.Printf("Erro ao ler mensagem de %s: %v", ResultTopic, err)
			continue
		}

		sourceKey := getHeaderValue(m.Headers, "source-key")
		// Observação: se workers mantêm esse header, não há risco de misturar diferentes textos.
		if sourceKey == "" {
			log.Printf("Mensagem de resultado sem 'source-key' no header. Ignorando.")
			if err := r.CommitMessages(context.Background(), m); err != nil {
				log.Printf("Falha ao commitar mensagem ignorada: %v", err)
			}
			continue
		}

		var isComplete bool
		var finalState *ConsolidationState

		mapMutex.Lock()
		state, ok := consolidationMap[sourceKey]
		if !ok {
			log.Printf("Estado de consolidação não encontrado para source-key: %s. Ignorando.", sourceKey)
			mapMutex.Unlock()
			if err := r.CommitMessages(context.Background(), m); err != nil {
				log.Printf("Falha ao commitar mensagem ignorada: %v", err)
			}
			continue
		}

		var chunkResult map[string]int
		if err := json.Unmarshal(m.Value, &chunkResult); err != nil {
			log.Printf("Erro ao decodificar resultado do chunk para source-key %s: %v", sourceKey, err)
			mapMutex.Unlock()
			// Não faz commit para tentar reprocessar
			continue
		}

		state.ReceivedChunks++
		for word, count := range chunkResult {
			state.WordCounts[word] += count
		}

		log.Printf("Resultado de chunk recebido para %s. Progresso: %d/%d", sourceKey, state.ReceivedChunks, state.TotalChunks)

		if state.ReceivedChunks == state.TotalChunks {
			isComplete = true
			finalState = state
			delete(consolidationMap, sourceKey)
		}
		mapMutex.Unlock()

		if isComplete {
			duration := time.Since(finalState.StartTime)

			// Converte o mapa para uma lista para ordenação
			wordList := make([]WordCount, 0, len(finalState.WordCounts))
			for word, count := range finalState.WordCounts {
				wordList = append(wordList, WordCount{Word: word, Count: count})
			}

			// Ordena a lista por contagem (decrescente)
			sort.Slice(wordList, func(i, j int) bool {
				if wordList[i].Count == wordList[j].Count {
					return wordList[i].Word < wordList[j].Word
				}
				return wordList[i].Count > wordList[j].Count
			})

			// Prepara o resultado para log e envio
			limit := TopNWords
			if len(wordList) < limit {
				limit = len(wordList)
			}
			topWords := wordList[:limit]

			resultData := map[string]interface{}{
				"task_id":          sourceKey,
				"processing_time":  duration.String(),
				"unique_words":     len(finalState.WordCounts),
				"top_words":        topWords,
				"performance_note": "Tempo total de processamento no servidor central.",
			}

			resultJSON, err := json.MarshalIndent(resultData, "", "  ")
			if err != nil {
				log.Printf("Erro ao serializar resultado final para %s: %v", sourceKey, err)
				// O estado já foi removido, então apenas logamos o erro.
				// A mensagem não será commitada, permitindo reprocessamento.
				continue
			}

			// Log do resultado consolidado
			log.Printf("--- RESULTADO CONSOLIDADO PARA %s ---", sourceKey)
			log.Printf("Resultado final:\n%s", string(resultJSON))
			log.Printf("-------------------------------------------------")

			// Envia o resultado para o tópico final
			msg := kafka.Message{
				Key:   []byte(sourceKey),
				Value: resultJSON,
				Headers: []kafka.Header{
					{Key: "task-id", Value: []byte(sourceKey)},
				},
			}

			err = writeWithRetry(context.Background(), func() *kafka.Writer { return newKafkaWriterForTopic(FinalResultTopic) }, &resultWriter, []kafka.Message{msg})
			if err != nil {
				log.Printf("Falha ao enviar resultado consolidado para %s após retries: %v", sourceKey, err)
				// Não deleta o estado para possível re-tentativa
				continue
			}

			log.Printf("Resultado consolidado para %s enviado para o tópico '%s'", sourceKey, FinalResultTopic)
		}

		if err := r.CommitMessages(context.Background(), m); err != nil {
			log.Printf("Falha ao commitar mensagem de resultado: %v", err)
		}
	}
}

func main() {
	var wg sync.WaitGroup
	wg.Add(2)

	go runChunker(&wg)
	go runConsolidator(&wg)

	log.Println("Servidor Central iniciado. Aguardando mensagens...")
	wg.Wait()
}