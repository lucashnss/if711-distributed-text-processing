package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

var kafkaBrokers = []string{
	"localhost:29092", // kafka1 EXTERNAL
	"localhost:29093", // kafka2 EXTERNAL
	"localhost:29094", // kafka3 EXTERNAL
	"localhost:29095", // kafka4 EXTERNAL
}

const (
	Topic        = "text-input"
	ResultsTopic = "results"
)

func newWriter() *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:      kafkaBrokers,
		Topic:        Topic,
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: 5 * time.Second,
	})
}
func newReader() *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: kafkaBrokers,
		Topic:   ResultsTopic,
		GroupID: fmt.Sprintf("client-group-%d", time.Now().UnixNano()), 
	})
}

func newMessage(taskID string, text string) kafka.Message {
	return kafka.Message{
		Key:   []byte(taskID),
		Value: []byte(text),
		Headers: []kafka.Header{
			{Key: "client-id", Value: []byte("word-count-client")},
			{Key: "task-id", Value: []byte(taskID)},
		},
	}
}

func main() {
	w := newWriter()
	defer w.Close()

	log.Println("Por favor, insira o texto para análise. Pressione Ctrl+D (Linux/macOS) ou Ctrl+Z e Enter (Windows) para finalizar.")

	textBytes, err := io.ReadAll(os.Stdin)
	if err != nil {
		log.Fatalf("Falha ao ler a entrada do terminal: %v", err)
	}
	textInput := strings.TrimSpace(string(textBytes))

	if textInput == "" {
		log.Println("Nenhuma entrada fornecida. Encerrando.")
		return
	}

	taskID := fmt.Sprintf("client-request-%d", time.Now().Unix())
	msg := newMessage(taskID, textInput)

	log.Printf("Enviando TaskID=%s para tópico '%s'", taskID, Topic)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := w.WriteMessages(ctx, msg); err != nil {
		log.Fatalf("Falha ao produzir mensagem: %v", err)
	}

	log.Printf("Mensagem entregue. Aguardando processamento do TaskID=%s no tópico '%s'", taskID, ResultsTopic)

	r := newReader()
	defer r.Close()

	// Contexto para aguardar o resultado por até 1 minuto
	resultCtx, resultCancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer resultCancel()

	for {
		msg, err := r.ReadMessage(resultCtx)
		if err != nil {
			if err == context.DeadlineExceeded {
				log.Println("Tempo de espera pelo resultado excedido. Nenhum resultado recebido.")
				break
			}
			log.Fatalf("Falha ao ler mensagem de resultado: %v", err)
		}

		var receivedTaskID string
		for _, header := range msg.Headers {
			if header.Key == "task-id" {
				receivedTaskID = string(header.Value)
				break
			}
		}

		if receivedTaskID == taskID {
			log.Printf("Resultado recebido para TaskID=%s:", taskID)

			var result map[string]interface{}
			if err := json.Unmarshal(msg.Value, &result); err != nil {
				// Se não for JSON, imprime como texto simples
				fmt.Printf("\n%s\n", string(msg.Value))
			} else {
				// Se for JSON, imprime de forma bonita
				fmt.Println("\n--- Análise de Palavras ---")
				for key, value := range result {
					if key == "top_words" {
						fmt.Printf("%-20s:\n", "Top Palavras")
						if topWords, ok := value.([]interface{}); ok {
							for _, item := range topWords {
								if wordMap, ok := item.(map[string]interface{}); ok {
									fmt.Printf("    %-16s: %.0f\n", wordMap["Word"], wordMap["Count"])
								}
							}
						}
					} else {
						fmt.Printf("%-20s: %v\n", key, value)
					}
				}
				fmt.Println("---------------------------")
			}
			break
		}
	}
}