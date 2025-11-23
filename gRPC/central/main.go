package main

import (
	"context"
	"flag"
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

// Variável dinâmica baseada na flag
var workerAddrs []string

type server struct {
	pb.UnimplementedAnalisadorServer
}

type Pair struct {
	Key   string
	Value int32
}

func (s *server) AnalisarTexto(ctx context.Context, req *pb.TextoRequest) (*pb.ResultadoResponse, error) {
	inicio := time.Now()
	textoTotal := req.GetTexto()
	
	palavras := strings.Fields(textoTotal)
	numWorkers := len(workerAddrs)
	
	// Prevenção de divisão por zero
	if numWorkers == 0 {
		return nil, fmt.Errorf("nenhum worker configurado")
	}

	chunkSize := (len(palavras) + numWorkers - 1) / numWorkers

	var wg sync.WaitGroup
	var mu sync.Mutex
	contagemGlobal := make(map[string]int32)

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
			
			conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				// Log de erro simplificado
				return
			}
			defer conn.Close()

			client := pb.NewAnalisadorClient(conn)
			resp, err := client.AnalisarTexto(context.Background(), &pb.TextoRequest{Texto: txt})
			if err != nil {
				return
			}

			mu.Lock()
			for k, v := range resp.Contagem {
				contagemGlobal[k] += v
			}
			mu.Unlock()
		}(addr, parteTexto)
	}

	wg.Wait()

	totalPalavras := int32(0)
	var listaPares []Pair

	for k, v := range contagemGlobal {
		totalPalavras += v
		listaPares = append(listaPares, Pair{k, v})
	}

	sort.Slice(listaPares, func(i, j int) bool {
		return listaPares[i].Value > listaPares[j].Value
	})

	limit := 10
	topList := []string{}
	for i := 0; i < len(listaPares) && i < limit; i++ {
		topList = append(topList, fmt.Sprintf("%s (%d)", listaPares[i].Key, listaPares[i].Value))
	}

	tempoTotal := time.Since(inicio).Nanoseconds()

	return &pb.ResultadoResponse{
		TotalPalavras:  totalPalavras,
		PalavrasUnicas: int32(len(contagemGlobal)),
		Contagem:       contagemGlobal,
		TopPalavras:    topList,
		Metricas: &pb.Metricas{
			TempoProcessamentoNanos: tempoTotal,
			WorkersUsados:           int32(numWorkers),
		},
	}, nil
}

func main() {
	// Flag para definir número de workers
	numWorkersPtr := flag.Int("n", 1, "Número de workers a utilizar")
	flag.Parse()

	// Gera os endereços baseados no número (50052, 50053, etc.)
	workerAddrs = []string{}
	for i := 0; i < *numWorkersPtr; i++ {
		port := 50052 + i
		addr := fmt.Sprintf("localhost:%d", port)
		workerAddrs = append(workerAddrs, addr)
	}

	port := ":50051"
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("falha ao escutar: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterAnalisadorServer(s, &server{})
	
	fmt.Printf("Central gRPC rodando na porta %s\n", port)
	fmt.Printf("Configurado para usar %d Workers: %v\n", *numWorkersPtr, workerAddrs)

	if err := s.Serve(lis); err != nil {
		log.Fatalf("falha ao servir: %v", err)
	}
}