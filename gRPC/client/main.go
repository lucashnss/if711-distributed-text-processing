package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	pb "stopwords_project/frequencia"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Conecta ao Central
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("não conectou: %v", err)
	}
	defer conn.Close()

	client := pb.NewAnalisadorClient(conn)
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("--- Cliente de Análise gRPC ---")

	for {
		fmt.Print("\nDigite o texto (ou 'sair'): ")
		texto, _ := reader.ReadString('\n')
		texto = strings.TrimSpace(texto)

		if strings.ToLower(texto) == "sair" {
			break
		}
		if texto == "" {
			continue
		}

		// Chamada gRPC
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		resp, err := client.AnalisarTexto(ctx, &pb.TextoRequest{Texto: texto})
		cancel()

		if err != nil {
			log.Printf("Erro na análise: %v", err)
			continue
		}

		// Exibir Resultados
		fmt.Println("\n--- Resultado ---")
		fmt.Printf("Total Palavras: %d\n", resp.TotalPalavras)
		fmt.Printf("Palavras Únicas: %d\n", resp.PalavrasUnicas)
		fmt.Printf("Top 5: %v\n", resp.Top_5)
		fmt.Printf("Tempo: %d µs\n", resp.Metricas.TempoProcessamentoNanos/1000)
	}
}