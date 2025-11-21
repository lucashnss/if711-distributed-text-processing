package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	pb "stopwords_project/frequencia"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Endereços dos Workers (aqui só temos 1, mas poderiam ser vários)
var workerAddrs = []string{"localhost:50052"}

type server struct {
	pb.UnimplementedAnalisadorServer
}

// Estrutura auxiliar para ordenação
type Pair struct {
	Key   string
	Value int32
}

func (s *server) AnalisarTexto(ctx context.Context, req *pb.TextoRequest) (*pb.ResultadoResponse, error) {
	inicio := time.Now()
	textoTotal := req.GetTexto()
	log.Printf("Central: Recebida requisição. Tamanho texto: %d", len(textoTotal))

	// 1. Dividir o Texto
	palavras := strings.Fields(textoTotal)
	numWorkers := len(workerAddrs)
	chunkSize := (len(palavras) + numWorkers - 1) / numWorkers

	var wg sync.WaitGroup
	var mu sync.Mutex
	contagemGlobal := make(map[string]int32)

	// 2. Distribuir para Workers via gRPC
	for i, addr := range workerAddrs {
		start := i * chunkSize
		end := start + chunkSize
		if start >= len(palavras) {
			break
		}
		if end > len(palavras) {
			end = len(palavras)
		}
		
		parteTexto := strings.Join(palavras[start:end], " ")

		wg.Add(1)
		go func(address, txt string) {
			defer wg.Done()
			
			// Conecta ao Worker
			conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Erro ao conectar no worker %s: %v", address, err)
				return
			}
			defer conn.Close()

			client := pb.NewAnalisadorClient(conn)
			resp, err := client.AnalisarTexto(context.Background(), &pb.TextoRequest{Texto: txt})
			if err != nil {
				log.Printf("Erro na chamada RPC ao worker: %v", err)
				return
			}

			// Consolida resultados (Mutex para segurança)
			mu.Lock()
			for k, v := range resp.Contagem {
				contagemGlobal[k] += v
			}
			mu.Unlock()
		}(addr, parteTexto)
	}

	wg.Wait()

	// 3. Calcular Estatísticas Finais
	totalPalavras := int32(0)
	var listaPares []Pair

	for k, v := range contagemGlobal {
		totalPalavras += v
		listaPares = append(listaPares, Pair{k, v})
	}

	// Ordenar para pegar Top 5
	sort.Slice(listaPares, func(i, j int) bool {
		return listaPares[i].Value > listaPares[j].Value
	})

	top5 := []string{}
	for i := 0; i < len(listaPares) && i < 5; i++ {
		top5 = append(top5, fmt.Sprintf("%s (%d)", listaPares[i].Key, listaPares[i].Value))
	}

	tempoTotal := time.Since(inicio).Nanoseconds()

	return &pb.ResultadoResponse{
		TotalPalavras:  totalPalavras,
		PalavrasUnicas: int32(len(contagemGlobal)),
		Contagem:       contagemGlobal,
		Top_5:          top5,
		Metricas: &pb.Metricas{
			TempoProcessamentoNanos: tempoTotal,
			WorkersUsados:           int32(numWorkers),
		},
	}, nil
}

func main() {
	port := ":50051"
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("falha ao escutar: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterAnalisadorServer(s, &server{})
	log.Printf("Central gRPC pronto em %s", port)

	if err := s.Serve(lis); err != nil {
		log.Fatalf("falha ao servir: %v", err)
	}
}