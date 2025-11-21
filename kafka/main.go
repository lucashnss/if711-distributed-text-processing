package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// Configuração do Kafka
const (
	BootstrapServers = "localhost:29092"
	Topic            = "text-input"    
)

func main() {
	brokerAddresses := []string{BootstrapServers}

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      brokerAddresses,
		Topic:        Topic,
		Balancer:     &kafka.LeastBytes{}, 
		WriteTimeout: 5 * time.Second,
	})
	defer w.Close()

	textInput := `
		Implementar uma aplicação cliente-servidor em que o cliente envia textos e um servidor central retorna as palavras mais frequentes e suas contagens. 
		O servidor deve processar o texto e remover stopwords antes da contagem. O processamento deve ser paralelo: o texto é dividido em partes e analisado por diferentes servidores. 
		O servidor central deve consolidar os resultados e retornar estatísticas globais, incluindo métricas de desempenho. 
		Stopwords são palavras comuns, como artigos (o, a, os, as), preposições (de, em, para) e conjunções (e, mas, ou), geralmente removidas de um texto para análise ou busca, pois carregam pouco significado sozinhas e podem poluir os resultados.
	`

	taskID := fmt.Sprintf("client-request-%d", time.Now().Unix())
	
	message := kafka.Message{
		Key:   []byte(taskID),
		Value: []byte(textInput),
		Headers: []kafka.Header{
			{Key: "client-id", Value: []byte("word-count-client")},
			{Key: "task-id", Value: []byte(taskID)},
		},
	}

	fmt.Printf("📦 Enviando texto com TaskID '%s' para o tópico '%s'...\n", taskID, Topic)
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := w.WriteMessages(ctx, message)
	
	if err != nil {
		log.Fatalf("❌ Falha ao produzir mensagem: %v", err)
	}

	fmt.Printf("✅ Mensagem entregue com sucesso para o Servidor Central!\n")
	fmt.Printf("Aguardando processamento do TaskID: %s\n", taskID)

}